use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_handoff_bundle_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-handoff-bundle | --render-latest-handoff-bundle-script | --run-latest-handoff-bundle | --verify-latest-handoff-bundle)";
const HANDOFF_BUNDLE_LATEST_SESSION_VERSION: &str = "1";
const HANDOFF_BUNDLE_LATEST_STATUS_VERSION: &str = "1";
const DECISION_PACKET_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_decision_packet_latest";
const HANDOFF_BUNDLE_BIN: &str = "copybot_tiny_live_activation_package_handoff_bundle";
const DECISION_PACKET_LATEST_PLAN_READY: &str =
    "tiny_live_package_decision_packet_latest_plan_ready";
const DECISION_PACKET_LATEST_VERIFY_OK: &str = "tiny_live_package_decision_packet_latest_verify_ok";
const HANDOFF_BUNDLE_VERIFY_OK: &str = "tiny_live_package_handoff_bundle_verify_ok";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-handoff-bundle handoff binds the latest persisted immutable package chain to the accepted handoff-bundle contract, but it never overrides Stage 3 or pre-activation refusal truth, it never runs the frozen live cutover controller by itself, and activation_authorized remains false";

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
    PlanLatestHandoffBundle,
    RenderLatestHandoffBundleScript,
    RunLatestHandoffBundle,
    VerifyLatestHandoffBundle,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageHandoffBundleLatestVerdict {
    TinyLivePackageHandoffBundleLatestPlanReady,
    TinyLivePackageHandoffBundleLatestScriptRendered,
    TinyLivePackageHandoffBundleLatestRefusedByMissingLatestChain,
    TinyLivePackageHandoffBundleLatestRefusedByInvalidLatestChain,
    TinyLivePackageHandoffBundleLatestRefusedByDriftedHandoffBundleContract,
    TinyLivePackageHandoffBundleLatestRefusedNowByStage3,
    TinyLivePackageHandoffBundleLatestRefusedNowByPreActivationGate,
    TinyLivePackageHandoffBundleLatestReadyForManualGoLiveReview,
    TinyLivePackageHandoffBundleLatestVerifyOk,
    TinyLivePackageHandoffBundleLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageHandoffBundleLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedHandoffBundleContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ReadyForManualGoLiveReview,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_decision_packet_latest_session_dir: String,
    nested_handoff_bundle_session_dir: String,
    downstream_decision_packet_session_dir: String,
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
    resolve_latest_decision_packet_command_summary: String,
    run_latest_decision_packet_command_summary: String,
    verify_latest_decision_packet_command_summary: String,
    downstream_handoff_bundle_command_summary: String,
    verify_handoff_bundle_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_decision_packet_latest_session_dir: String,
    nested_handoff_bundle_session_dir: String,
    downstream_decision_packet_session_dir: String,
    result: LivePackageHandoffBundleLatestResult,
    reason: String,
    latest_decision_packet_plan_verdict: Option<String>,
    latest_decision_packet_verdict: Option<String>,
    handoff_bundle_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_decision_packet_plan_step: Option<PackageHandoffBundleLatestStepArtifact>,
    latest_decision_packet_step: Option<PackageHandoffBundleLatestStepArtifact>,
    handoff_bundle_step: Option<PackageHandoffBundleLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageHandoffBundleLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_decision_packet_plan_report_path: PathBuf,
    latest_decision_packet_report_path: PathBuf,
    handoff_bundle_report_path: PathBuf,
    nested_decision_packet_latest_session_dir: PathBuf,
    nested_handoff_bundle_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageHandoffBundleLatestVerdict,
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
    handoff_bundle_latest_session_path: Option<String>,
    handoff_bundle_latest_status_path: Option<String>,
    handoff_bundle_latest_report_path: Option<String>,
    latest_decision_packet_plan_report_path: Option<String>,
    latest_decision_packet_report_path: Option<String>,
    handoff_bundle_report_path: Option<String>,
    nested_decision_packet_latest_session_dir: Option<String>,
    nested_handoff_bundle_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_decision_packet_plan_verdict: Option<String>,
    latest_decision_packet_verdict: Option<String>,
    handoff_bundle_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_decision_packet_command_summary: Option<String>,
    run_latest_decision_packet_command_summary: Option<String>,
    verify_latest_decision_packet_command_summary: Option<String>,
    downstream_handoff_bundle_command_summary: Option<String>,
    verify_handoff_bundle_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestHandoffBundleSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    downstream_decision_packet_session_dir: PathBuf,
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
}

#[derive(Debug, Clone)]
struct ResolvedLatestHandoffBundle {
    snapshot: LatestHandoffBundleSnapshot,
    latest_decision_packet_plan_raw: Value,
    latest_decision_packet_plan: DecisionPacketLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedHandoffBundleContract,
}

#[derive(Debug, Clone)]
struct LatestHandoffBundleResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestHandoffBundleSnapshot>,
    latest_decision_packet_plan_raw: Option<Value>,
    latest_decision_packet_plan_verdict: Option<String>,
    latest_decision_packet_plan_reason: Option<String>,
    latest_decision_packet_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct DecisionPacketLatestReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
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
    nested_decision_packet_session_dir: Option<String>,
    latest_execute_verdict: Option<String>,
    decision_packet_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    activation_authorized: bool,
    explicit_statement: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct HandoffBundleManifestEntry {
    label: String,
    path: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HandoffBundleReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    decision_packet_session_dir: String,
    execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    handoff_bundle_session_path: Option<String>,
    handoff_bundle_status_path: Option<String>,
    result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    verify_decision_packet_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    handoff_bundle_summary: Option<String>,
    manifest: Vec<HandoffBundleManifestEntry>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HandoffBundleSessionView {
    decision_packet_session_dir: String,
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
    verify_decision_packet_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    manifest: Vec<HandoffBundleManifestEntry>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HandoffBundleStatusView {
    session_dir: String,
    decision_packet_session_dir: String,
    result: String,
    reason: String,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    manifest: Vec<HandoffBundleManifestEntry>,
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
            "--plan-latest-handoff-bundle" => {
                set_mode(&mut mode, Mode::PlanLatestHandoffBundle, arg.as_str())?
            }
            "--render-latest-handoff-bundle-script" => set_mode(
                &mut mode,
                Mode::RenderLatestHandoffBundleScript,
                arg.as_str(),
            )?,
            "--run-latest-handoff-bundle" => {
                set_mode(&mut mode, Mode::RunLatestHandoffBundle, arg.as_str())?
            }
            "--verify-latest-handoff-bundle" => {
                set_mode(&mut mode, Mode::VerifyLatestHandoffBundle, arg.as_str())?
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
        Mode::PlanLatestHandoffBundle
            | Mode::RenderLatestHandoffBundleScript
            | Mode::RunLatestHandoffBundle
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestHandoffBundleScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestHandoffBundle | Mode::VerifyLatestHandoffBundle
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
        Mode::PlanLatestHandoffBundle => plan_latest_handoff_bundle_report(&config)?,
        Mode::RenderLatestHandoffBundleScript => {
            render_latest_handoff_bundle_script_report(&config)?
        }
        Mode::RunLatestHandoffBundle => run_latest_handoff_bundle_report(&config)?,
        Mode::VerifyLatestHandoffBundle => verify_latest_handoff_bundle_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_handoff_bundle_report(config: &Config) -> Result<PackageHandoffBundleLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_handoff_bundle_latest.session"));
    let paths = handoff_bundle_latest_paths(&suggested_session_dir);

    match resolve_latest_handoff_bundle(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_handoff_bundle",
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-decision-packet {} lineage and the accepted handoff-bundle contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_decision_packet_latest_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_handoff_bundle",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_handoff_bundle_script_report(
    config: &Config,
) -> Result<PackageHandoffBundleLatestReport> {
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
    let paths = handoff_bundle_latest_paths(&suggested_session_dir);

    match resolve_latest_handoff_bundle(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_handoff_bundle}\n{verify_handoff_bundle}\n",
                    run_latest = run_latest_decision_packet_command_summary(
                        root,
                        &paths.nested_decision_packet_latest_session_dir,
                    ),
                    verify_latest = verify_latest_decision_packet_command_summary(
                        &paths.nested_decision_packet_latest_session_dir,
                    ),
                    run_handoff_bundle = downstream_handoff_bundle_command_summary(
                        &resolved.snapshot,
                        &paths.nested_handoff_bundle_session_dir,
                    ),
                    verify_handoff_bundle = verify_handoff_bundle_command_summary(
                        &resolved.snapshot,
                        &paths.nested_handoff_bundle_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_handoff_bundle_script",
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestScriptRendered,
                format!(
                    "rendered latest-handoff-bundle handoff script to {} using the accepted latest-decision-packet and handoff-bundle contracts",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_handoff_bundle_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_handoff_bundle_report(config: &Config) -> Result<PackageHandoffBundleLatestReport> {
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
            "refusing to overwrite existing latest-handoff-bundle session dir {}",
            session_dir.display()
        );
    }
    let paths = handoff_bundle_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-handoff-bundle")?;

    let resolution = resolve_latest_handoff_bundle(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_handoff_bundle_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => run_latest_handoff_bundle_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_handoff_bundle_report(
    config: &Config,
) -> Result<PackageHandoffBundleLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = handoff_bundle_latest_paths(session_dir);

    let session: PackageHandoffBundleLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-handoff-bundle session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ))
        }
    };
    let status: PackageHandoffBundleLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-handoff-bundle status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ))
        }
    };
    let stored_report: PackageHandoffBundleLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-handoff-bundle report artifact {}: {error}",
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
        "latest-handoff-bundle root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-handoff-bundle session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-handoff-bundle status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-handoff-bundle report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_decision_packet_latest_session_dir,
        &paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        "latest-handoff-bundle session nested_decision_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_decision_packet_latest_session_dir,
        &paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        "latest-handoff-bundle status nested_decision_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_decision_packet_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_decision_packet_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-handoff-bundle report nested_decision_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_handoff_bundle_session_dir,
        &paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        "latest-handoff-bundle session nested_handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_handoff_bundle_session_dir,
        &paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        "latest-handoff-bundle status nested_handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_handoff_bundle_session_dir.as_deref(),
        Some(
            paths
                .nested_handoff_bundle_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-handoff-bundle report nested_handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-handoff-bundle session downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-handoff-bundle status downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .downstream_decision_packet_session_dir
            .as_deref(),
        Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-handoff-bundle report downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.handoff_bundle_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-handoff-bundle report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.handoff_bundle_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-handoff-bundle report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.handoff_bundle_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-handoff-bundle report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-handoff-bundle session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-handoff-bundle status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-handoff-bundle report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_handoff_bundle",
        "latest-handoff-bundle stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-handoff-bundle stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-handoff-bundle stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-handoff-bundle stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.activation_authorized != status.activation_authorized {
        mismatches.push(format!(
            "latest-handoff-bundle stored report activation_authorized {} does not match status {}",
            stored_report.activation_authorized, status.activation_authorized
        ));
    }
    compare_option_string(
        stored_report.latest_decision_packet_plan_verdict.as_deref(),
        status.latest_decision_packet_plan_verdict.as_deref(),
        "stored report latest_decision_packet_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_decision_packet_verdict.as_deref(),
        status.latest_decision_packet_verdict.as_deref(),
        "stored report latest_decision_packet_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.handoff_bundle_verdict.as_deref(),
        status.handoff_bundle_verdict.as_deref(),
        "stored report handoff_bundle_verdict",
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

    let resolution = resolve_latest_handoff_bundle(&root, &paths);
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

            let stored_latest_plan: DecisionPacketLatestReportView = load_required_step_json(
                &status.latest_decision_packet_plan_step,
                &paths.latest_decision_packet_plan_report_path,
                "latest_decision_packet_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_decision_packet_plan_step.as_ref(),
                &paths.latest_decision_packet_plan_report_path,
                "plan_latest_decision_packet",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_decision_packet_plan_step",
                &mut mismatches,
            );
            compare_decision_packet_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_decision_packet_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_decision_packet_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_decision_packet_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_decision_packet_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_decision_packet_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: DecisionPacketLatestReportView = load_required_step_json(
                &status.latest_decision_packet_step,
                &paths.latest_decision_packet_report_path,
                "latest_decision_packet_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_decision_packet_step.as_ref(),
                &paths.latest_decision_packet_report_path,
                "run_latest_decision_packet",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_decision_packet_step",
                &mut mismatches,
            );
            compare_decision_packet_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_decision_packet_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_decision_packet_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_decision_packet_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_decision_packet_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_decision_packet_verdict",
                &mut mismatches,
            );

            let actual_latest_run: DecisionPacketLatestReportView = match load_json(
                &nested_decision_packet_latest_report_path(
                    &paths.nested_decision_packet_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-decision-packet report under {}: {error}",
                        paths.nested_decision_packet_latest_session_dir.display()
                    ));
                    fallback_decision_packet_latest_copy()
                }
            };
            compare_decision_packet_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                DECISION_PACKET_LATEST_BIN,
                &latest_decision_packet_verify_args(
                    &paths.nested_decision_packet_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-decision-packet session under {}: {error}",
                        paths.nested_decision_packet_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: DecisionPacketLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-decision-packet verify json")?;
                if latest_verify.verdict != DECISION_PACKET_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-decision-packet verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_handoff_bundle: HandoffBundleReportView = load_required_step_json(
                &status.handoff_bundle_step,
                &paths.handoff_bundle_report_path,
                "handoff_bundle_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.handoff_bundle_step.as_ref(),
                &paths.handoff_bundle_report_path,
                "run_live_package_handoff_bundle",
                &stored_handoff_bundle.verdict,
                &stored_handoff_bundle.reason,
                stored_handoff_bundle.generated_at,
                "stored handoff_bundle_step",
                &mut mismatches,
            );
            compare_handoff_bundle_run_against_snapshot(
                &stored_handoff_bundle,
                &resolved.snapshot,
                &paths.nested_handoff_bundle_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.handoff_bundle_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored status handoff_bundle_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.handoff_bundle_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored report handoff_bundle_verdict",
                &mut mismatches,
            );

            let nested_handoff_bundle_session: HandoffBundleSessionView = match load_json(
                &nested_handoff_bundle_session_path(&paths.nested_handoff_bundle_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested handoff-bundle session under {}: {error}",
                        paths.nested_handoff_bundle_session_dir.display()
                    ));
                    fallback_handoff_bundle_session(
                        &resolved.snapshot,
                        &paths.nested_handoff_bundle_session_dir,
                    )
                }
            };
            let nested_handoff_bundle_status: HandoffBundleStatusView = match load_json(
                &nested_handoff_bundle_status_path(&paths.nested_handoff_bundle_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested handoff-bundle status under {}: {error}",
                        paths.nested_handoff_bundle_session_dir.display()
                    ));
                    fallback_handoff_bundle_status(
                        &resolved.snapshot,
                        &paths.nested_handoff_bundle_session_dir,
                    )
                }
            };
            compare_handoff_bundle_copy_against_nested_truth(
                &stored_handoff_bundle,
                &nested_handoff_bundle_session,
                &nested_handoff_bundle_status,
                &mut mismatches,
            );

            let handoff_bundle_verify_raw = match run_json_command(
                HANDOFF_BUNDLE_BIN,
                &handoff_bundle_verify_args(
                    &resolved.snapshot,
                    &paths.nested_handoff_bundle_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested handoff-bundle session under {}: {error}",
                        paths.nested_handoff_bundle_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !handoff_bundle_verify_raw.is_null() {
                let handoff_bundle_verify: HandoffBundleReportView =
                    serde_json::from_value(handoff_bundle_verify_raw)
                        .context("failed parsing nested handoff-bundle verify json")?;
                compare_option_string(
                    stored_handoff_bundle
                        .frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored handoff-bundle copy frozen_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if handoff_bundle_verify.verdict != HANDOFF_BUNDLE_VERIFY_OK {
                    mismatches.push(format!(
                        "nested handoff-bundle verification is non-green: {}",
                        handoff_bundle_verify.reason
                    ));
                }
            }

            let expected_result = handoff_bundle_latest_result_from_handoff_bundle(
                nested_handoff_bundle_status.result.as_str(),
            )
            .unwrap_or(LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract);
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-handoff-bundle result {} does not match nested handoff-bundle result {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
            compare_string(
                &status.reason,
                &nested_handoff_bundle_status.reason,
                "stored status reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(expected_result),
                "stored status operator_next_action_summary",
                &mut mismatches,
            );
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must remain false for latest-handoff-bundle"
                        .to_string(),
                );
            }
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                nested_handoff_bundle_status
                    .current_pre_activation_gate_verdict
                    .as_deref(),
                "stored status current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                nested_handoff_bundle_status
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
            if failure.latest_decision_packet_plan_raw.is_some() {
                let stored_latest_plan: DecisionPacketLatestReportView = load_required_step_json(
                    &status.latest_decision_packet_plan_step,
                    &paths.latest_decision_packet_plan_report_path,
                    "latest_decision_packet_plan_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_decision_packet_plan_step.as_ref(),
                    &paths.latest_decision_packet_plan_report_path,
                    "plan_latest_decision_packet",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_decision_packet_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_decision_packet_plan_verdict.as_deref(),
                    failure.latest_decision_packet_plan_verdict.as_deref(),
                    "stored status latest_decision_packet_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report.latest_decision_packet_plan_verdict.as_deref(),
                    failure.latest_decision_packet_plan_verdict.as_deref(),
                    "stored report latest_decision_packet_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_decision_packet_verdict.as_deref(),
                None,
                "stored status latest_decision_packet_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_decision_packet_verdict.as_deref(),
                None,
                "stored report latest_decision_packet_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.handoff_bundle_verdict.as_deref(),
                None,
                "stored status handoff_bundle_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.handoff_bundle_verdict.as_deref(),
                None,
                "stored report handoff_bundle_verdict on failure",
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
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must be false on failure".to_string(),
                );
            }
            if stored_report.activation_authorized {
                mismatches.push(
                    "stored report activation_authorized must be false on failure".to_string(),
                );
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyOk
    } else {
        TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-handoff-bundle session under {} is coherent with the current latest immutable chain and the accepted handoff-bundle contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-handoff-bundle verification failed".to_string())
    };

    Ok(PackageHandoffBundleLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_handoff_bundle".to_string(),
        verdict,
        reason,
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status
            .historical_execute_frozen_session_dir
            .clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        handoff_bundle_latest_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_latest_status_path: Some(paths.status_path.display().to_string()),
        handoff_bundle_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_decision_packet_plan_report_path: Some(
            paths
                .latest_decision_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_decision_packet_report_path: Some(
            paths.latest_decision_packet_report_path.display().to_string(),
        ),
        handoff_bundle_report_path: Some(paths.handoff_bundle_report_path.display().to_string()),
        nested_decision_packet_latest_session_dir: Some(
            paths
                .nested_decision_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_handoff_bundle_session_dir: Some(
            paths.nested_handoff_bundle_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_decision_packet_plan_verdict: status.latest_decision_packet_plan_verdict.clone(),
        latest_decision_packet_verdict: status.latest_decision_packet_verdict.clone(),
        handoff_bundle_verdict: status.handoff_bundle_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_decision_packet_command_summary: Some(
            session.resolve_latest_decision_packet_command_summary.clone(),
        ),
        run_latest_decision_packet_command_summary: Some(
            session.run_latest_decision_packet_command_summary.clone(),
        ),
        verify_latest_decision_packet_command_summary: Some(
            session.verify_latest_decision_packet_command_summary.clone(),
        ),
        downstream_handoff_bundle_command_summary: Some(
            session.downstream_handoff_bundle_command_summary.clone(),
        ),
        verify_handoff_bundle_command_summary: Some(session.verify_handoff_bundle_command_summary.clone()),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted handoff-bundle contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_handoff_bundle_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    resolved: &ResolvedLatestHandoffBundle,
) -> Result<PackageHandoffBundleLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_decision_packet_plan_report_path,
        &resolved.latest_decision_packet_plan_raw,
    )?;

    let latest_decision_packet_plan_step = Some(step_artifact(
        &paths.latest_decision_packet_plan_report_path,
        "plan_latest_decision_packet",
        &resolved.latest_decision_packet_plan.verdict,
        &resolved.latest_decision_packet_plan.reason,
        resolved.latest_decision_packet_plan.generated_at,
    ));

    let latest_decision_packet_run_raw = match run_json_command(
        DECISION_PACKET_LATEST_BIN,
        &latest_decision_packet_run_args(root, &paths.nested_decision_packet_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
                format!(
                    "failed running nested latest-decision-packet handoff under {}: {error}",
                    paths.nested_decision_packet_latest_session_dir.display()
                ),
                latest_decision_packet_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_handoff_bundle",
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
        &paths.latest_decision_packet_report_path,
        &latest_decision_packet_run_raw,
    )?;
    let latest_decision_packet_run: DecisionPacketLatestReportView =
        serde_json::from_value(latest_decision_packet_run_raw)
            .context("failed parsing nested latest-decision-packet run json")?;
    let latest_decision_packet_step = Some(step_artifact(
        &paths.latest_decision_packet_report_path,
        "run_latest_decision_packet",
        &latest_decision_packet_run.verdict,
        &latest_decision_packet_run.reason,
        latest_decision_packet_run.generated_at,
    ));

    if let Some(terminal_failure) =
        latest_decision_packet_terminal_failure(&latest_decision_packet_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_decision_packet_run.reason.clone(),
            latest_decision_packet_plan_step,
            latest_decision_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_handoff_bundle",
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
    compare_decision_packet_latest_run_against_snapshot(
        &latest_decision_packet_run,
        &resolved.snapshot,
        &paths.nested_decision_packet_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
            run_mismatches[0].clone(),
            latest_decision_packet_plan_step,
            latest_decision_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_handoff_bundle",
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

    let latest_decision_packet_verify: DecisionPacketLatestReportView = match run_json_command(
        DECISION_PACKET_LATEST_BIN,
        &latest_decision_packet_verify_args(&paths.nested_decision_packet_latest_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested latest-decision-packet verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
                format!(
                    "failed verifying nested latest-decision-packet handoff under {}: {error}",
                    paths.nested_decision_packet_latest_session_dir.display()
                ),
                latest_decision_packet_plan_step,
                latest_decision_packet_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_handoff_bundle",
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
    if latest_decision_packet_verify.verdict != DECISION_PACKET_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
            latest_decision_packet_verify.reason,
            latest_decision_packet_plan_step,
            latest_decision_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_handoff_bundle",
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

    let handoff_bundle_run_raw = match run_json_command(
        HANDOFF_BUNDLE_BIN,
        &handoff_bundle_run_args(&resolved.snapshot, &paths.nested_handoff_bundle_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
                format!(
                    "failed running downstream handoff-bundle contract under {}: {error}",
                    paths.nested_handoff_bundle_session_dir.display()
                ),
                latest_decision_packet_plan_step,
                latest_decision_packet_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_handoff_bundle",
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
    persist_json(&paths.handoff_bundle_report_path, &handoff_bundle_run_raw)?;
    let handoff_bundle_run: HandoffBundleReportView =
        serde_json::from_value(handoff_bundle_run_raw)
            .context("failed parsing nested handoff-bundle run json")?;
    let handoff_bundle_step = Some(step_artifact(
        &paths.handoff_bundle_report_path,
        "run_live_package_handoff_bundle",
        &handoff_bundle_run.verdict,
        &handoff_bundle_run.reason,
        handoff_bundle_run.generated_at,
    ));

    let mut handoff_bundle_mismatches = Vec::new();
    compare_handoff_bundle_run_against_snapshot(
        &handoff_bundle_run,
        &resolved.snapshot,
        &paths.nested_handoff_bundle_session_dir,
        &mut handoff_bundle_mismatches,
    );
    if !handoff_bundle_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
            handoff_bundle_mismatches[0].clone(),
            latest_decision_packet_plan_step,
            latest_decision_packet_step,
            handoff_bundle_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_handoff_bundle",
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

    let handoff_bundle_verify: HandoffBundleReportView = match run_json_command(
        HANDOFF_BUNDLE_BIN,
        &handoff_bundle_verify_args(&resolved.snapshot, &paths.nested_handoff_bundle_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested handoff-bundle verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
                format!(
                    "failed verifying downstream handoff-bundle contract under {}: {error}",
                    paths.nested_handoff_bundle_session_dir.display()
                ),
                latest_decision_packet_plan_step,
                latest_decision_packet_step,
                handoff_bundle_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_handoff_bundle",
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
    if handoff_bundle_verify.verdict != HANDOFF_BUNDLE_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract,
            handoff_bundle_verify.reason,
            latest_decision_packet_plan_step,
            latest_decision_packet_step,
            handoff_bundle_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_handoff_bundle",
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

    let result = handoff_bundle_latest_result_from_handoff_bundle(
        handoff_bundle_run.result.as_deref().unwrap_or_default(),
    )
    .unwrap_or(LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract);
    let status = PackageHandoffBundleLatestStatus {
        status_version: HANDOFF_BUNDLE_LATEST_STATUS_VERSION.to_string(),
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
        nested_decision_packet_latest_session_dir: paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        nested_handoff_bundle_session_dir: paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: resolved
            .snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: handoff_bundle_run.reason.clone(),
        latest_decision_packet_plan_verdict: Some(
            resolved.latest_decision_packet_plan.verdict.clone(),
        ),
        latest_decision_packet_verdict: Some(latest_decision_packet_run.verdict.clone()),
        handoff_bundle_verdict: Some(handoff_bundle_run.verdict.clone()),
        current_pre_activation_gate_verdict: handoff_bundle_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: handoff_bundle_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_decision_packet_plan_step,
        latest_decision_packet_step,
        handoff_bundle_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_handoff_bundle",
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

fn run_latest_handoff_bundle_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    failure: &LatestHandoffBundleResolutionFailure,
) -> Result<PackageHandoffBundleLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_decision_packet_plan_raw {
        persist_json(&paths.latest_decision_packet_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_handoff_bundle",
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

fn resolve_latest_handoff_bundle(
    root: &Path,
    paths: &PackageHandoffBundleLatestPaths,
) -> std::result::Result<ResolvedLatestHandoffBundle, LatestHandoffBundleResolutionFailure> {
    let latest_decision_packet_plan_raw = match run_json_command(
        DECISION_PACKET_LATEST_BIN,
        &latest_decision_packet_plan_args(root, &paths.nested_decision_packet_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestHandoffBundleResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_decision_packet_plan_raw: None,
                latest_decision_packet_plan_verdict: None,
                latest_decision_packet_plan_reason: None,
                latest_decision_packet_plan_generated_at: None,
            });
        }
    };
    let latest_decision_packet_plan: DecisionPacketLatestReportView =
        serde_json::from_value(latest_decision_packet_plan_raw.clone()).map_err(|error| {
            LatestHandoffBundleResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-decision-packet plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_decision_packet_plan_raw: Some(latest_decision_packet_plan_raw.clone()),
                latest_decision_packet_plan_verdict: None,
                latest_decision_packet_plan_reason: None,
                latest_decision_packet_plan_generated_at: None,
            }
        })?;

    let snapshot = parse_snapshot_from_decision_packet_latest(&latest_decision_packet_plan).ok();
    if latest_decision_packet_plan.verdict != DECISION_PACKET_LATEST_PLAN_READY {
        return Err(LatestHandoffBundleResolutionFailure {
            kind: map_decision_packet_latest_failure_kind(&latest_decision_packet_plan.verdict),
            reason: latest_decision_packet_plan.reason.clone(),
            snapshot,
            latest_decision_packet_plan_raw: Some(latest_decision_packet_plan_raw),
            latest_decision_packet_plan_verdict: Some(latest_decision_packet_plan.verdict),
            latest_decision_packet_plan_reason: Some(latest_decision_packet_plan.reason),
            latest_decision_packet_plan_generated_at: Some(
                latest_decision_packet_plan.generated_at,
            ),
        });
    }

    let snapshot = parse_snapshot_from_decision_packet_latest(&latest_decision_packet_plan)
        .map_err(|error| LatestHandoffBundleResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: error.to_string(),
            snapshot: None,
            latest_decision_packet_plan_raw: Some(latest_decision_packet_plan_raw.clone()),
            latest_decision_packet_plan_verdict: Some(latest_decision_packet_plan.verdict.clone()),
            latest_decision_packet_plan_reason: Some(latest_decision_packet_plan.reason.clone()),
            latest_decision_packet_plan_generated_at: Some(
                latest_decision_packet_plan.generated_at,
            ),
        })?;

    let mut mismatches = Vec::new();
    compare_decision_packet_latest_plan_against_snapshot(
        &latest_decision_packet_plan,
        &snapshot,
        &paths.nested_decision_packet_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestHandoffBundleResolutionFailure {
            kind: ResolutionFailureKind::DriftedHandoffBundleContract,
            reason: mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_decision_packet_plan_raw: Some(latest_decision_packet_plan_raw),
            latest_decision_packet_plan_verdict: Some(latest_decision_packet_plan.verdict),
            latest_decision_packet_plan_reason: Some(latest_decision_packet_plan.reason),
            latest_decision_packet_plan_generated_at: Some(
                latest_decision_packet_plan.generated_at,
            ),
        });
    }

    Ok(ResolvedLatestHandoffBundle {
        snapshot,
        latest_decision_packet_plan_raw,
        latest_decision_packet_plan,
    })
}

fn parse_snapshot_from_decision_packet_latest(
    view: &DecisionPacketLatestReportView,
) -> Result<LatestHandoffBundleSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "latest-decision-packet plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    if view.activation_authorized {
        bail!("latest-decision-packet plan must remain planning-safe with activation_authorized=false");
    }
    Ok(LatestHandoffBundleSnapshot {
        latest_top_step_name,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        downstream_decision_packet_session_dir: PathBuf::from(required_option_string(
            view.nested_decision_packet_session_dir.as_ref(),
            "nested_decision_packet_session_dir",
        )?),
        historical_execute_frozen_session_dir: PathBuf::from(required_option_string(
            view.historical_execute_frozen_session_dir.as_ref(),
            "historical_execute_frozen_session_dir",
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
            .ok_or_else(|| anyhow!("latest-decision-packet plan is missing wrapper_timeout_ms"))?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("latest-decision-packet plan is missing service_status_max_staleness_ms")
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
    verdict: TinyLivePackageHandoffBundleLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    resolved: &ResolvedLatestHandoffBundle,
) -> PackageHandoffBundleLatestReport {
    PackageHandoffBundleLatestReport {
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
        handoff_bundle_latest_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_latest_status_path: Some(paths.status_path.display().to_string()),
        handoff_bundle_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_decision_packet_plan_report_path: Some(
            paths
                .latest_decision_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_decision_packet_report_path: Some(
            paths
                .latest_decision_packet_report_path
                .display()
                .to_string(),
        ),
        handoff_bundle_report_path: Some(paths.handoff_bundle_report_path.display().to_string()),
        nested_decision_packet_latest_session_dir: Some(
            paths
                .nested_decision_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_handoff_bundle_session_dir: Some(
            paths
                .nested_handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            resolved
                .snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_decision_packet_plan_verdict: Some(
            resolved.latest_decision_packet_plan.verdict.clone(),
        ),
        latest_decision_packet_verdict: None,
        handoff_bundle_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_decision_packet_command_summary: Some(
            resolve_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        run_latest_decision_packet_command_summary: Some(
            run_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        verify_latest_decision_packet_command_summary: Some(
            verify_latest_decision_packet_command_summary(
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        downstream_handoff_bundle_command_summary: Some(downstream_handoff_bundle_command_summary(
            &resolved.snapshot,
            &paths.nested_handoff_bundle_session_dir,
        )),
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &resolved.snapshot,
            &paths.nested_handoff_bundle_session_dir,
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
    paths: &PackageHandoffBundleLatestPaths,
    failure: &LatestHandoffBundleResolutionFailure,
) -> PackageHandoffBundleLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageHandoffBundleLatestReport {
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
        handoff_bundle_latest_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_latest_status_path: Some(paths.status_path.display().to_string()),
        handoff_bundle_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_decision_packet_plan_report_path: Some(
            paths
                .latest_decision_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_decision_packet_report_path: Some(
            paths
                .latest_decision_packet_report_path
                .display()
                .to_string(),
        ),
        handoff_bundle_report_path: Some(paths.handoff_bundle_report_path.display().to_string()),
        nested_decision_packet_latest_session_dir: Some(
            paths
                .nested_decision_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_handoff_bundle_session_dir: Some(
            paths
                .nested_handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
        }),
        latest_decision_packet_plan_verdict: failure.latest_decision_packet_plan_verdict.clone(),
        latest_decision_packet_verdict: None,
        handoff_bundle_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_decision_packet_command_summary: Some(
            resolve_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        run_latest_decision_packet_command_summary: Some(
            run_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        verify_latest_decision_packet_command_summary: Some(
            verify_latest_decision_packet_command_summary(
                &paths.nested_decision_packet_latest_session_dir,
            ),
        ),
        downstream_handoff_bundle_command_summary: snapshot.map(|value| {
            downstream_handoff_bundle_command_summary(
                value,
                &paths.nested_handoff_bundle_session_dir,
            )
        }),
        verify_handoff_bundle_command_summary: snapshot.map(|value| {
            verify_handoff_bundle_command_summary(value, &paths.nested_handoff_bundle_session_dir)
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

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    resolved: Option<&ResolvedLatestHandoffBundle>,
    failure: Option<&LatestHandoffBundleResolutionFailure>,
) -> PackageHandoffBundleLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageHandoffBundleLatestSession {
        session_version: HANDOFF_BUNDLE_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_decision_packet_latest_session_dir: paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        nested_handoff_bundle_session_dir: paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
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
        resolve_latest_decision_packet_command_summary:
            resolve_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            ),
        run_latest_decision_packet_command_summary: run_latest_decision_packet_command_summary(
            root,
            &paths.nested_decision_packet_latest_session_dir,
        ),
        verify_latest_decision_packet_command_summary:
            verify_latest_decision_packet_command_summary(
                &paths.nested_decision_packet_latest_session_dir,
            ),
        downstream_handoff_bundle_command_summary: snapshot
            .map(|value| {
                downstream_handoff_bundle_command_summary(
                    value,
                    &paths.nested_handoff_bundle_session_dir,
                )
            })
            .unwrap_or_else(|| "<handoff-bundle-run-unavailable>".to_string()),
        verify_handoff_bundle_command_summary: snapshot
            .map(|value| {
                verify_handoff_bundle_command_summary(
                    value,
                    &paths.nested_handoff_bundle_session_dir,
                )
            })
            .unwrap_or_else(|| "<handoff-bundle-verify-unavailable>".to_string()),
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
    verdict: TinyLivePackageHandoffBundleLatestVerdict,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    session: &PackageHandoffBundleLatestSession,
    status: &PackageHandoffBundleLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageHandoffBundleLatestReport {
    PackageHandoffBundleLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status.historical_execute_frozen_session_dir.clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        handoff_bundle_latest_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_latest_status_path: Some(paths.status_path.display().to_string()),
        handoff_bundle_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_decision_packet_plan_report_path: Some(
            paths
                .latest_decision_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_decision_packet_report_path: Some(
            paths
                .latest_decision_packet_report_path
                .display()
                .to_string(),
        ),
        handoff_bundle_report_path: Some(paths.handoff_bundle_report_path.display().to_string()),
        nested_decision_packet_latest_session_dir: Some(
            session.nested_decision_packet_latest_session_dir.clone(),
        ),
        nested_handoff_bundle_session_dir: Some(session.nested_handoff_bundle_session_dir.clone()),
        downstream_decision_packet_session_dir: Some(
            session.downstream_decision_packet_session_dir.clone(),
        ),
        latest_decision_packet_plan_verdict: status.latest_decision_packet_plan_verdict.clone(),
        latest_decision_packet_verdict: status.latest_decision_packet_verdict.clone(),
        handoff_bundle_verdict: status.handoff_bundle_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_decision_packet_command_summary: Some(
            session
                .resolve_latest_decision_packet_command_summary
                .clone(),
        ),
        run_latest_decision_packet_command_summary: Some(
            session.run_latest_decision_packet_command_summary.clone(),
        ),
        verify_latest_decision_packet_command_summary: Some(
            session
                .verify_latest_decision_packet_command_summary
                .clone(),
        ),
        downstream_handoff_bundle_command_summary: Some(
            session.downstream_handoff_bundle_command_summary.clone(),
        ),
        verify_handoff_bundle_command_summary: Some(
            session.verify_handoff_bundle_command_summary.clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        activation_authorized: status.activation_authorized,
        explicit_statement: session.explicit_statement.clone(),
    }
}

fn handoff_bundle_latest_paths(session_dir: &Path) -> PackageHandoffBundleLatestPaths {
    PackageHandoffBundleLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_handoff_bundle_latest.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_handoff_bundle_latest.status.json"),
        report_path: session_dir.join("tiny_live_activation_package_handoff_bundle_latest.report.json"),
        latest_decision_packet_plan_report_path: session_dir.join(
            "tiny_live_activation_package_handoff_bundle_latest.decision_packet_latest_plan.report.json",
        ),
        latest_decision_packet_report_path: session_dir.join(
            "tiny_live_activation_package_handoff_bundle_latest.decision_packet_latest.report.json",
        ),
        handoff_bundle_report_path: session_dir
            .join("tiny_live_activation_package_handoff_bundle_latest.handoff_bundle.report.json"),
        nested_decision_packet_latest_session_dir: session_dir
            .join("tiny_live_activation_package_handoff_bundle_latest.decision_packet_latest_session"),
        nested_handoff_bundle_session_dir: session_dir
            .join("tiny_live_activation_package_handoff_bundle_latest.handoff_bundle_session"),
    }
}

fn expected_downstream_decision_packet_session_dir(
    paths: &PackageHandoffBundleLatestPaths,
) -> PathBuf {
    paths
        .nested_decision_packet_latest_session_dir
        .join("tiny_live_activation_package_decision_packet_latest.decision_packet_session")
}

fn latest_decision_packet_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-decision-packet".to_string(),
        "--json".to_string(),
    ]
}

fn latest_decision_packet_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-decision-packet".to_string(),
        "--json".to_string(),
    ]
}

fn latest_decision_packet_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-decision-packet".to_string(),
        "--json".to_string(),
    ]
}

fn handoff_bundle_run_args(
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-handoff-bundle".to_string(),
        "--json".to_string(),
    ]
}

fn handoff_bundle_verify_args(
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-handoff-bundle".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_decision_packet_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{DECISION_PACKET_LATEST_BIN} --root {} --session-dir {} --plan-latest-decision-packet --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_decision_packet_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{DECISION_PACKET_LATEST_BIN} --root {} --session-dir {} --run-latest-decision-packet --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_decision_packet_command_summary(session_dir: &Path) -> String {
    format!(
        "{DECISION_PACKET_LATEST_BIN} --session-dir {} --verify-latest-decision-packet --json",
        shell_escape_path(session_dir),
    )
}

fn downstream_handoff_bundle_command_summary(
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{HANDOFF_BUNDLE_BIN} --decision-packet-session-dir {} --session-dir {} --run-live-package-handoff-bundle --json",
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_handoff_bundle_command_summary(
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{HANDOFF_BUNDLE_BIN} --decision-packet-session-dir {} --session-dir {} --verify-live-package-handoff-bundle --json",
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_decision_packet_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_decision_packet_latest.report.json")
}

fn nested_handoff_bundle_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_handoff_bundle.session.json")
}

fn nested_handoff_bundle_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_handoff_bundle.status.json")
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
            "failed writing latest-handoff-bundle script to {}",
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
) -> PackageHandoffBundleLatestStepArtifact {
    PackageHandoffBundleLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageHandoffBundleLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-handoff-bundle status"))?;
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
    step: Option<&PackageHandoffBundleLatestStepArtifact>,
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
            "{label} is missing from latest-handoff-bundle status"
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

fn compare_decision_packet_latest_plan_against_snapshot(
    stored: &DecisionPacketLatestReportView,
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(DECISION_PACKET_LATEST_PLAN_READY),
        "stored latest-decision-packet plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_decision_packet",
        "stored latest-decision-packet plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-decision-packet plan latest_top_step_name",
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
        "stored latest-decision-packet plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-decision-packet plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-decision-packet plan nested_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-decision-packet plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-decision-packet plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-decision-packet plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-decision-packet plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-decision-packet plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-decision-packet plan service_status_max_staleness_ms",
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
        "stored latest-decision-packet plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-decision-packet plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-decision-packet plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-decision-packet plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_decision_packet_latest_run_against_snapshot(
    stored: &DecisionPacketLatestReportView,
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_decision_packet",
        "stored latest-decision-packet run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-decision-packet run latest_top_step_name",
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
        "stored latest-decision-packet run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-decision-packet run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-decision-packet run nested_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-decision-packet run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-decision-packet run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-decision-packet run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-decision-packet run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-decision-packet run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-decision-packet run service_status_max_staleness_ms",
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
        "stored latest-decision-packet run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-decision-packet run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-decision-packet run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-decision-packet run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_decision_packet_latest_copy_matches_actual(
    stored: &DecisionPacketLatestReportView,
    actual: &DecisionPacketLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-decision-packet copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-decision-packet copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-decision-packet copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-decision-packet copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-decision-packet copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-decision-packet copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_decision_packet_session_dir.as_deref(),
        actual.nested_decision_packet_session_dir.as_deref(),
        "stored latest-decision-packet copy nested_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-decision-packet copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-decision-packet copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-decision-packet copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-decision-packet copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-decision-packet copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_handoff_bundle_run_against_snapshot(
    stored: &HandoffBundleReportView,
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_handoff_bundle",
        "stored handoff-bundle run mode",
        mismatches,
    );
    compare_string(
        &stored.decision_packet_session_dir,
        &snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "stored handoff-bundle run decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(
            snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored handoff-bundle run execute_frozen_session_dir",
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
        "stored handoff-bundle run turn_green_session_dir",
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
        "stored handoff-bundle run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored handoff-bundle run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored handoff-bundle run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored handoff-bundle run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored handoff-bundle run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored handoff-bundle run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored handoff-bundle run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored handoff-bundle run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_session_path.as_deref(),
        Some(
            nested_handoff_bundle_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored handoff-bundle run handoff_bundle_session_path",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_status_path.as_deref(),
        Some(
            nested_handoff_bundle_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored handoff-bundle run handoff_bundle_status_path",
        mismatches,
    );
    compare_option_string(
        stored.verify_decision_packet_command_summary.as_deref(),
        Some(
            handoff_bundle_verify_decision_packet_command_summary(
                &snapshot.downstream_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored handoff-bundle run verify_decision_packet_command_summary",
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
        "stored handoff-bundle run frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_handoff_bundle_copy_against_nested_truth(
    stored: &HandoffBundleReportView,
    nested_session: &HandoffBundleSessionView,
    nested_status: &HandoffBundleStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.decision_packet_session_dir,
        &nested_session.decision_packet_session_dir,
        "stored handoff-bundle copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(nested_session.execute_frozen_session_dir.as_str()),
        "stored handoff-bundle copy execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored handoff-bundle copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored handoff-bundle copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored handoff-bundle copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored handoff-bundle copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        handoff_bundle_run_verdict_for_result(&nested_status.result),
        "stored handoff-bundle copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_result.as_deref(),
        nested_status.decision_packet_result.as_deref(),
        "stored handoff-bundle copy decision_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored handoff-bundle copy execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored handoff-bundle copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored handoff-bundle copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored.operator_checklist_summary.as_deref(),
        Some(nested_status.operator_checklist_summary.as_str()),
        "stored handoff-bundle copy operator_checklist_summary",
        mismatches,
    );
    compare_option_string(
        stored.operator_runbook_summary.as_deref(),
        Some(nested_status.operator_runbook_summary.as_str()),
        "stored handoff-bundle copy operator_runbook_summary",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_summary.as_deref(),
        Some(nested_status.handoff_bundle_summary.as_str()),
        "stored handoff-bundle copy handoff_bundle_summary",
        mismatches,
    );
    compare_option_string(
        stored.verify_decision_packet_command_summary.as_deref(),
        Some(
            nested_session
                .verify_decision_packet_command_summary
                .as_str(),
        ),
        "stored handoff-bundle copy verify_decision_packet_command_summary",
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
        "stored handoff-bundle copy frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored handoff-bundle copy reason",
        mismatches,
    );
    compare_manifest_entries(
        &stored.manifest,
        &nested_status.manifest,
        "stored handoff-bundle copy manifest",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored handoff-bundle copy explicit_statement",
        mismatches,
    );
}

fn compare_manifest_entries(
    actual: &[HandoffBundleManifestEntry],
    expected: &[HandoffBundleManifestEntry],
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} does not match expected immutable membership"
        ));
    }
}

fn compare_session_against_snapshot(
    session: &PackageHandoffBundleLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    snapshot: &LatestHandoffBundleSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_decision_packet_command_summary,
        &resolve_latest_decision_packet_command_summary(
            root,
            &paths.nested_decision_packet_latest_session_dir,
        ),
        "stored session resolve_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_decision_packet_command_summary,
        &run_latest_decision_packet_command_summary(
            root,
            &paths.nested_decision_packet_latest_session_dir,
        ),
        "stored session run_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_decision_packet_command_summary,
        &verify_latest_decision_packet_command_summary(
            &paths.nested_decision_packet_latest_session_dir,
        ),
        "stored session verify_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_handoff_bundle_command_summary,
        &downstream_handoff_bundle_command_summary(
            snapshot,
            &paths.nested_handoff_bundle_session_dir,
        ),
        "stored session downstream_handoff_bundle_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_handoff_bundle_command_summary,
        &verify_handoff_bundle_command_summary(snapshot, &paths.nested_handoff_bundle_session_dir),
        "stored session verify_handoff_bundle_command_summary",
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
        &session.nested_decision_packet_latest_session_dir,
        &paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_decision_packet_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_handoff_bundle_session_dir,
        &paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        "stored session nested_handoff_bundle_session_dir",
        mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "stored session downstream_decision_packet_session_dir",
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
    report: &PackageHandoffBundleLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    snapshot: &LatestHandoffBundleSnapshot,
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
            .resolve_latest_decision_packet_command_summary
            .as_deref(),
        Some(
            resolve_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.run_latest_decision_packet_command_summary.as_deref(),
        Some(
            run_latest_decision_packet_command_summary(
                root,
                &paths.nested_decision_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report run_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_decision_packet_command_summary
            .as_deref(),
        Some(
            verify_latest_decision_packet_command_summary(
                &paths.nested_decision_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_latest_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_handoff_bundle_command_summary.as_deref(),
        Some(
            downstream_handoff_bundle_command_summary(
                snapshot,
                &paths.nested_handoff_bundle_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_handoff_bundle_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_handoff_bundle_command_summary.as_deref(),
        Some(
            verify_handoff_bundle_command_summary(
                snapshot,
                &paths.nested_handoff_bundle_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_handoff_bundle_command_summary",
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
        report.handoff_bundle_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report handoff_bundle_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.handoff_bundle_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report handoff_bundle_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.handoff_bundle_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report handoff_bundle_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_decision_packet_plan_report_path.as_deref(),
        Some(
            paths
                .latest_decision_packet_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_decision_packet_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_decision_packet_report_path.as_deref(),
        Some(
            paths
                .latest_decision_packet_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_decision_packet_report_path",
        mismatches,
    );
    compare_option_string(
        report.handoff_bundle_report_path.as_deref(),
        Some(
            paths
                .handoff_bundle_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report handoff_bundle_report_path",
        mismatches,
    );
    compare_option_string(
        report.nested_decision_packet_latest_session_dir.as_deref(),
        Some(
            paths
                .nested_decision_packet_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_decision_packet_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_handoff_bundle_session_dir.as_deref(),
        Some(
            paths
                .nested_handoff_bundle_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_handoff_bundle_session_dir",
        mismatches,
    );
    compare_option_string(
        report.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report downstream_decision_packet_session_dir",
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
    session: &PackageHandoffBundleLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    failure: &LatestHandoffBundleResolutionFailure,
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
        &session.nested_decision_packet_latest_session_dir,
        &paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_decision_packet_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_handoff_bundle_session_dir,
        &paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        "stored session nested_handoff_bundle_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageHandoffBundleLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    failure: &LatestHandoffBundleResolutionFailure,
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
        "run_latest_handoff_bundle",
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

fn map_decision_packet_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_decision_packet_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_decision_packet_latest_refused_by_drifted_decision_packet_contract" => {
            ResolutionFailureKind::DriftedHandoffBundleContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_decision_packet_terminal_failure(
    report: &DecisionPacketLatestReportView,
) -> Option<LivePackageHandoffBundleLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageHandoffBundleLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageHandoffBundleLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_decision_packet_contract") => {
            Some(LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract)
        }
        _ => None,
    }
}

fn handoff_bundle_latest_result_from_handoff_bundle(
    result: &str,
) -> Option<LivePackageHandoffBundleLatestResult> {
    match result {
        "ready_for_manual_go_live_review" => {
            Some(LivePackageHandoffBundleLatestResult::ReadyForManualGoLiveReview)
        }
        "refused_now_by_stage3" => Some(LivePackageHandoffBundleLatestResult::RefusedNowByStage3),
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageHandoffBundleLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some(LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract)
        }
        _ => None,
    }
}

fn handoff_bundle_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "ready_for_manual_go_live_review" => {
            Some("tiny_live_package_handoff_bundle_ready_for_manual_go_live_review")
        }
        "refused_now_by_stage3" => Some("tiny_live_package_handoff_bundle_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_handoff_bundle_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_handoff_bundle_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageHandoffBundleLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageHandoffBundleLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageHandoffBundleLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedHandoffBundleContract => {
            LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageHandoffBundleLatestResult,
) -> TinyLivePackageHandoffBundleLatestVerdict {
    match result {
        LivePackageHandoffBundleLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByMissingLatestChain
        }
        LivePackageHandoffBundleLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByInvalidLatestChain
        }
        LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByDriftedHandoffBundleContract
        }
        LivePackageHandoffBundleLatestResult::RefusedNowByStage3 => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedNowByStage3
        }
        LivePackageHandoffBundleLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedNowByPreActivationGate
        }
        LivePackageHandoffBundleLatestResult::ReadyForManualGoLiveReview => {
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestReadyForManualGoLiveReview
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageHandoffBundleLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageHandoffBundleLatestResult) -> String {
    match result {
        LivePackageHandoffBundleLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt launch-packet or handoff-bundle lineage by hand".to_string()
        }
        LivePackageHandoffBundleLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-decision-packet / handoff-bundle path".to_string()
        }
        LivePackageHandoffBundleLatestResult::RefusedByDriftedHandoffBundleContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-decision-packet / handoff-bundle contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageHandoffBundleLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-handoff-bundle refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageHandoffBundleLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-handoff-bundle refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageHandoffBundleLatestResult::ReadyForManualGoLiveReview => {
            "the latest immutable chain now resolves into the accepted handoff-bundle contract and is ready for manual go-live review; this wrapper still did not run or authorize the frozen controller".to_string()
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

fn handoff_bundle_verify_decision_packet_command_summary(
    decision_packet_session_dir: &Path,
) -> String {
    format!(
        "verify immutable decision-packet session under {} before freezing the final go-live handoff bundle",
        decision_packet_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    resolved: Option<&ResolvedLatestHandoffBundle>,
    result: LivePackageHandoffBundleLatestResult,
    reason: String,
    latest_decision_packet_plan_step: Option<PackageHandoffBundleLatestStepArtifact>,
    latest_decision_packet_step: Option<PackageHandoffBundleLatestStepArtifact>,
    handoff_bundle_step: Option<PackageHandoffBundleLatestStepArtifact>,
) -> PackageHandoffBundleLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageHandoffBundleLatestStatus {
        status_version: HANDOFF_BUNDLE_LATEST_STATUS_VERSION.to_string(),
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
        nested_decision_packet_latest_session_dir: paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        nested_handoff_bundle_session_dir: paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result,
        reason,
        latest_decision_packet_plan_verdict: resolved
            .map(|value| value.latest_decision_packet_plan.verdict.clone()),
        latest_decision_packet_verdict: latest_decision_packet_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        handoff_bundle_verdict: handoff_bundle_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_decision_packet_plan_step,
        latest_decision_packet_step,
        handoff_bundle_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageHandoffBundleLatestPaths,
    failure: &LatestHandoffBundleResolutionFailure,
) -> PackageHandoffBundleLatestStatus {
    PackageHandoffBundleLatestStatus {
        status_version: HANDOFF_BUNDLE_LATEST_STATUS_VERSION.to_string(),
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
        historical_execute_frozen_session_dir: failure.snapshot.as_ref().map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_decision_packet_latest_session_dir: paths
            .nested_decision_packet_latest_session_dir
            .display()
            .to_string(),
        nested_handoff_bundle_session_dir: paths
            .nested_handoff_bundle_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_decision_packet_plan_verdict: failure.latest_decision_packet_plan_verdict.clone(),
        latest_decision_packet_verdict: None,
        handoff_bundle_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_decision_packet_plan_step: failure.latest_decision_packet_plan_raw.as_ref().map(
            |_| {
                step_artifact(
                    &paths.latest_decision_packet_plan_report_path,
                    "plan_latest_decision_packet",
                    failure
                        .latest_decision_packet_plan_verdict
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_decision_packet_plan_reason
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_decision_packet_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            },
        ),
        latest_decision_packet_step: None,
        handoff_bundle_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageHandoffBundleLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-handoff-bundle verification failed".to_string());
    PackageHandoffBundleLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_handoff_bundle".to_string(),
        verdict: TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid,
        reason,
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
        handoff_bundle_latest_session_path: Some(
            handoff_bundle_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        handoff_bundle_latest_status_path: Some(
            handoff_bundle_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        handoff_bundle_latest_report_path: Some(
            handoff_bundle_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_decision_packet_plan_report_path: None,
        latest_decision_packet_report_path: None,
        handoff_bundle_report_path: None,
        nested_decision_packet_latest_session_dir: None,
        nested_handoff_bundle_session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_decision_packet_plan_verdict: None,
        latest_decision_packet_verdict: None,
        handoff_bundle_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_decision_packet_command_summary: None,
        run_latest_decision_packet_command_summary: None,
        verify_latest_decision_packet_command_summary: None,
        downstream_handoff_bundle_command_summary: None,
        verify_handoff_bundle_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted handoff-bundle contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_decision_packet_latest_copy() -> DecisionPacketLatestReportView {
    DecisionPacketLatestReportView {
        generated_at: Utc::now(),
        mode: "<missing>".to_string(),
        verdict: "<missing>".to_string(),
        reason: "<missing>".to_string(),
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
        session_dir: None,
        nested_decision_packet_session_dir: None,
        latest_execute_verdict: None,
        decision_packet_plan_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        activation_authorized: false,
        explicit_statement: None,
    }
}

fn fallback_handoff_bundle_session(
    snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> HandoffBundleSessionView {
    HandoffBundleSessionView {
        decision_packet_session_dir: snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
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
        verify_decision_packet_command_summary:
            handoff_bundle_verify_decision_packet_command_summary(
                &snapshot.downstream_decision_packet_session_dir,
            ),
        frozen_live_cutover_controller_command_summary: snapshot
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        operator_checklist_summary:
            "follow the frozen go-live checklist before any manual live cutover review".to_string(),
        operator_runbook_summary:
            "use the frozen live cutover runbook referenced by the verified decision packet".to_string(),
        handoff_bundle_summary:
            "bundle the verified decision packet, checklist, and runbook into one immutable manual-review packet"
                .to_string(),
        manifest: Vec::new(),
        explicit_statement:
            "the verified decision-packet session is the only handoff input here; this archival bundle freezes one immutable go-live dossier over the exact frozen live cutover contract without enabling production execution"
                .to_string(),
    }
}

fn fallback_handoff_bundle_status(
    _snapshot: &LatestHandoffBundleSnapshot,
    session_dir: &Path,
) -> HandoffBundleStatusView {
    HandoffBundleStatusView {
        session_dir: session_dir.display().to_string(),
        decision_packet_session_dir: "<missing>".to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing>".to_string(),
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        operator_checklist_summary:
            "follow the frozen go-live checklist before any manual live cutover review".to_string(),
        operator_runbook_summary:
            "use the frozen live cutover runbook referenced by the verified decision packet".to_string(),
        handoff_bundle_summary:
            "bundle the verified decision packet, checklist, and runbook into one immutable manual-review packet"
                .to_string(),
        manifest: Vec::new(),
        explicit_statement:
            "this handoff bundle is read-only and archival; it never executes the frozen controller itself"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageHandoffBundleLatestReport) -> String {
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
    if let Some(value) = &report.nested_decision_packet_latest_session_dir {
        lines.push(format!("nested_decision_packet_latest_session_dir={value}"));
    }
    if let Some(value) = &report.downstream_decision_packet_session_dir {
        lines.push(format!("downstream_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_handoff_bundle_session_dir {
        lines.push(format!("nested_handoff_bundle_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_handoff_bundle_command_summary {
        lines.push(format!("downstream_handoff_bundle_command_summary={value}"));
    }
    if let Some(value) = &report.reviewed_frozen_live_cutover_controller_command_summary {
        lines.push(format!(
            "reviewed_frozen_live_cutover_controller_command_summary={value}"
        ));
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
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

    struct FakeHandoffBundleLatestFixture {
        temp_dir: PathBuf,
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
        decision_packet_latest_plan_json_path: PathBuf,
        decision_packet_latest_run_json_path: PathBuf,
        decision_packet_latest_verify_json_path: PathBuf,
        handoff_bundle_run_json_path: PathBuf,
        handoff_bundle_verify_json_path: PathBuf,
        handoff_bundle_session_json_path: PathBuf,
        handoff_bundle_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeHandoffBundleLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestHandoffBundle,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestHandoffBundleScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestHandoffBundle,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestHandoffBundle,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_decision_packet_lineage(
    ) {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_plan_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_handoff_bundle_report(&fixture.plan_config()).unwrap();
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report.nested_decision_packet_latest_session_dir.as_deref(),
            Some(
                paths
                    .nested_decision_packet_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(
                expected_downstream_decision_packet_session_dir(&paths)
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_handoff_bundle_command_summary
            .as_deref()
            .unwrap()
            .contains(HANDOFF_BUNDLE_BIN));
    }

    #[test]
    fn render_mode_emits_exact_downstream_handoff_bundle_contract_not_second_controller_path() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_render_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_handoff_bundle_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(DECISION_PACKET_LATEST_BIN));
        assert!(script.contains(HANDOFF_BUNDLE_BIN));
        assert!(script.contains("--run-live-package-handoff-bundle"));
        assert!(script.contains("--verify-live-package-handoff-bundle"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "handoff_bundle_latest_missing",
                "tiny_live_package_decision_packet_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByMissingLatestChain,
            ),
            (
                "handoff_bundle_latest_invalid",
                "tiny_live_package_decision_packet_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByInvalidLatestChain,
            ),
            (
                "handoff_bundle_latest_below",
                "tiny_live_package_decision_packet_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_handoff_bundle_latest_fixture(
                label,
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_review",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.decision_packet_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.decision_packet_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn run_mode_refuses_when_nested_launch_packet_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_decision_packet_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.decision_packet_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.decision_packet_latest_run_json_path, &run).unwrap();

        let report = run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedByDriftedHandoffBundleContract
        );
        assert!(report
            .reason
            .contains("stored latest-decision-packet run package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_handoff_bundle_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_handoff_bundle_latest_fixture(
                "handoff_bundle_latest_verify_tampered_copy",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_review",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

            let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.handoff_bundle_report_path).unwrap();
            report["reason"] = json!("tampered stored handoff-bundle copy reason");
            persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

            let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored handoff-bundle copy reason")));
        }

        {
            let fixture = fake_handoff_bundle_latest_fixture(
                "handoff_bundle_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_review",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

            let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_decision_packet_latest_report_path(
                &paths.nested_decision_packet_latest_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_decision_packet_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_decision_packet_explicit_statement_drifts() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_verify_launch_packet_explicit_statement_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_decision_packet_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest launch packet explicit statement");
        persist_json(&paths.latest_decision_packet_report_path, &report).unwrap();

        let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-decision-packet copy explicit_statement")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_handoff_bundle_decision_packet_result_drifts() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_verify_decision_packet_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.handoff_bundle_report_path).unwrap();
        report["decision_packet_result"] = json!("tampered_decision_packet_result");
        persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

        let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored handoff-bundle copy decision_packet_result") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_handoff_bundle_execute_frozen_result_drifts() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_verify_current_authorization_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.handoff_bundle_report_path).unwrap();
        report["execute_frozen_result"] = json!("tampered_execute_frozen_result");
        persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

        let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored handoff-bundle copy execute_frozen_result") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_handoff_bundle_verdict_drifts() {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_verify_handoff_bundle_verdict_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.handoff_bundle_report_path).unwrap();
        report["verdict"] = json!("tampered_handoff_bundle_verdict");
        persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

        let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored handoff-bundle copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_handoff_bundle_current_live_cutover_controller_summary_drifts(
    ) {
        let fixture = fake_handoff_bundle_latest_fixture(
            "handoff_bundle_latest_verify_current_live_cutover_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_review",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.handoff_bundle_report_path).unwrap();
        report["frozen_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

        let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored handoff-bundle copy frozen_live_cutover_controller_command_summary",
            )
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_handoff_bundle_latest_fixture(
                "handoff_bundle_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_handoff_bundle_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestPlanReady
            );
            assert!(!plan.activation_authorized);

            let run = run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_handoff_bundle_latest_fixture(
                "handoff_bundle_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify = verify_latest_handoff_bundle_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_handoff_bundle_latest_fixture(
                "handoff_bundle_latest_green",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_review",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_handoff_bundle_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageHandoffBundleLatestVerdict::TinyLivePackageHandoffBundleLatestReadyForManualGoLiveReview
            );
            assert_eq!(
                run.result.as_deref(),
                Some("ready_for_manual_go_live_review")
            );
            assert!(!run.activation_authorized);
            assert!(run
                .explicit_statement
                .contains("never runs the frozen live cutover controller"));
        }
    }

    fn fake_handoff_bundle_latest_fixture(
        label: &str,
        decision_packet_latest_run_result: &str,
        handoff_bundle_result: &str,
    ) -> FakeHandoffBundleLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("root");
        let wrapper_session_dir = temp_dir.join("handoff-bundle-latest-session");
        let output_script_path = temp_dir.join("handoff-bundle-latest.sh");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let historical_execute_frozen_session_dir =
            temp_dir.join("historical-execute-frozen-session");
        let turn_green_session_dir = temp_dir.join("historical-turn-green-session");
        let launch_packet_session_dir = temp_dir.join("historical-launch-packet-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend");
        let decision_packet_latest_plan_json_path =
            temp_dir.join("decision_packet_latest.plan.json");
        let decision_packet_latest_run_json_path = temp_dir.join("decision_packet_latest.run.json");
        let decision_packet_latest_verify_json_path =
            temp_dir.join("decision_packet_latest.verify.json");
        let handoff_bundle_run_json_path = temp_dir.join("handoff_bundle.run.json");
        let handoff_bundle_verify_json_path = temp_dir.join("handoff_bundle.verify.json");
        let handoff_bundle_session_json_path = temp_dir.join("handoff_bundle.session.json");
        let handoff_bundle_status_json_path = temp_dir.join("handoff_bundle.status.json");
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

        let fixture = FakeHandoffBundleLatestFixture {
            temp_dir,
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
            decision_packet_latest_plan_json_path,
            decision_packet_latest_run_json_path,
            decision_packet_latest_verify_json_path,
            handoff_bundle_run_json_path,
            handoff_bundle_verify_json_path,
            handoff_bundle_session_json_path,
            handoff_bundle_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.decision_packet_latest_plan_json_path,
            &default_decision_packet_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_latest_run_json_path,
            &default_decision_packet_latest_run_report(&fixture, decision_packet_latest_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_latest_verify_json_path,
            &default_decision_packet_latest_verify_report(
                &fixture,
                decision_packet_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_session_json_path,
            &default_handoff_bundle_session(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_status_json_path,
            &default_handoff_bundle_status(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_run_json_path,
            &default_handoff_bundle_run_report(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_verify_json_path,
            &default_handoff_bundle_verify_report(&fixture, handoff_bundle_result),
        )
        .unwrap();

        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        write_fake_decision_packet_latest_command(
            &fixture.bin_dir.join(DECISION_PACKET_LATEST_BIN),
            &fixture.decision_packet_latest_plan_json_path,
            &fixture.decision_packet_latest_run_json_path,
            &fixture.decision_packet_latest_verify_json_path,
            &fixture.root,
            &paths.nested_decision_packet_latest_session_dir,
        );
        write_fake_handoff_bundle_command(
            &fixture.bin_dir.join(HANDOFF_BUNDLE_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_handoff_bundle_session_dir,
        );

        fixture
    }

    fn default_decision_packet_latest_plan_report(
        fixture: &FakeHandoffBundleLatestFixture,
    ) -> Value {
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_decision_packet",
            "verdict": DECISION_PACKET_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-decision-packet contract",
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_decision_packet_latest_session_dir.display().to_string(),
            "nested_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest decision packet wrapper plan"
        })
    }

    fn default_decision_packet_latest_run_report(
        fixture: &FakeHandoffBundleLatestFixture,
        result: &str,
    ) -> Value {
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "runnable_when_gate_truth_turns_green" => (
                "tiny_live_package_decision_packet_latest_runnable_when_gate_truth_turns_green",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_decision_packet_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_decision_packet_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_decision_packet_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_decision_packet_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_decision_packet_latest_refused_by_drifted_decision_packet_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_decision_packet",
            "verdict": verdict,
            "reason": format!("latest decision packet {result}"),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_decision_packet_latest_session_dir.display().to_string(),
            "nested_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "result": result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest decision packet wrapper run"
        })
    }

    fn default_decision_packet_latest_verify_report(
        fixture: &FakeHandoffBundleLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_decision_packet_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_decision_packet");
        report["verdict"] = json!(DECISION_PACKET_LATEST_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn default_handoff_bundle_session(fixture: &FakeHandoffBundleLatestFixture) -> Value {
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let snapshot = LatestHandoffBundleSnapshot {
            latest_top_step_name: "clerestory_certificate".to_string(),
            latest_top_session_dir: fixture.latest_top_session_dir.clone(),
            downstream_decision_packet_session_dir: expected_downstream_decision_packet_session_dir(
                &paths,
            ),
            historical_execute_frozen_session_dir: fixture
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
        };
        json!({
            "session_version": "1",
            "planned_at": Utc::now(),
            "decision_packet_session_dir": snapshot.downstream_decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": snapshot.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": snapshot.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": snapshot.launch_packet_session_dir.display().to_string(),
            "package_dir": snapshot.package_dir.display().to_string(),
            "install_root": snapshot.install_root.display().to_string(),
            "target_service_name": snapshot.target_service_name,
            "backend_command": snapshot.backend_command,
            "wrapper_timeout_ms": snapshot.wrapper_timeout_ms,
            "service_status_max_staleness_ms": snapshot.service_status_max_staleness_ms,
            "session_dir": handoff_bundle_latest_paths(&fixture.wrapper_session_dir).nested_handoff_bundle_session_dir.display().to_string(),
            "verify_decision_packet_command_summary": handoff_bundle_verify_decision_packet_command_summary(&snapshot.downstream_decision_packet_session_dir),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_checklist_summary": "follow the frozen go-live checklist before any manual live cutover review",
            "operator_runbook_summary": "use the frozen live cutover runbook referenced by the verified decision packet",
            "handoff_bundle_summary": "bundle the verified decision packet, checklist, and runbook into one immutable manual-review packet",
            "manifest": [],
            "explicit_statement": "the verified decision-packet session is the only handoff input here; this archival bundle freezes one immutable go-live dossier over the exact frozen live cutover contract without enabling production execution"
        })
    }

    fn default_handoff_bundle_status(
        fixture: &FakeHandoffBundleLatestFixture,
        result: &str,
    ) -> Value {
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let (reason, gate_verdict, gate_reason) =
            match result {
                "ready_for_manual_go_live_review" => (
                    "the verified decision-packet session remains coherent and the immutable handoff bundle is ready for manual go-live review",
                    json!("green"),
                    json!("ok"),
                ),
                "refused_now_by_stage3" => (
                    "the verified decision-packet session is still coherent, but the immutable handoff bundle remains refused now because current Stage 3 is non-green",
                    json!("blocked"),
                    json!("stage3 blocked"),
                ),
                _ => (
                    "the verified decision-packet session is still coherent, but the immutable handoff bundle remains refused now because the current pre-activation gate is non-green",
                    json!("blocked"),
                    json!("pre gate blocked"),
                ),
            };
        json!({
            "status_version": "1",
            "updated_at": Utc::now(),
            "session_dir": paths.nested_handoff_bundle_session_dir.display().to_string(),
            "decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "result": result,
            "reason": reason,
            "decision_packet_result": if result == "ready_for_manual_go_live_review" { json!("runnable_when_gate_truth_turns_green") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "execute_frozen_result": "completed_keep_running",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "operator_checklist_summary": "follow the frozen go-live checklist before any manual live cutover review",
            "operator_runbook_summary": "use the frozen live cutover runbook referenced by the verified decision packet",
            "handoff_bundle_summary": "bundle the verified decision packet, checklist, and runbook into one immutable manual-review packet",
            "manifest": [],
            "explicit_statement": "this handoff bundle is read-only and archival; it never executes the frozen controller itself"
        })
    }

    fn default_handoff_bundle_run_report(
        fixture: &FakeHandoffBundleLatestFixture,
        result: &str,
    ) -> Value {
        let paths = handoff_bundle_latest_paths(&fixture.wrapper_session_dir);
        let status = default_handoff_bundle_status(fixture, result);
        let verdict = match result {
            "ready_for_manual_go_live_review" => {
                "tiny_live_package_handoff_bundle_ready_for_manual_go_live_review"
            }
            "refused_now_by_stage3" => "tiny_live_package_handoff_bundle_refused_now_by_stage3",
            _ => "tiny_live_package_handoff_bundle_refused_now_by_pre_activation_gate",
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_handoff_bundle",
            "verdict": verdict,
            "reason": status["reason"],
            "decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_handoff_bundle_session_dir.display().to_string(),
            "handoff_bundle_session_path": nested_handoff_bundle_session_path(&paths.nested_handoff_bundle_session_dir).display().to_string(),
            "handoff_bundle_status_path": nested_handoff_bundle_status_path(&paths.nested_handoff_bundle_session_dir).display().to_string(),
            "result": result,
            "decision_packet_result": status["decision_packet_result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "verify_decision_packet_command_summary": handoff_bundle_verify_decision_packet_command_summary(&expected_downstream_decision_packet_session_dir(&paths)),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_checklist_summary": status["operator_checklist_summary"],
            "operator_runbook_summary": status["operator_runbook_summary"],
            "handoff_bundle_summary": status["handoff_bundle_summary"],
            "manifest": status["manifest"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_handoff_bundle_verify_report(
        fixture: &FakeHandoffBundleLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_handoff_bundle_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_handoff_bundle");
        report["verdict"] = json!(HANDOFF_BUNDLE_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification proves only whether the frozen controller is executable now; it never runs that controller");
        report
    }

    fn write_fake_decision_packet_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --root {} \"*) : ;;\n  *\" --verify-latest-decision-packet \"*) : ;;\n  *) echo 'unexpected root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-decision-packet session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-decision-packet \"*) mode='plan';;\n  *\" --run-latest-decision-packet \"*) mode='run';;\n  *\" --verify-latest-decision-packet \"*) mode='verify';;\n  *) echo 'unexpected latest-decision-packet mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_decision_packet_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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

    fn write_fake_handoff_bundle_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        launch_packet_session_dir: &Path,
        session_dir: &Path,
    ) {
        let nested_session_path = nested_handoff_bundle_session_path(session_dir);
        let nested_status_path = nested_handoff_bundle_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected handoff-bundle launch-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected handoff-bundle session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-handoff-bundle \"*) mode='run';;\n  *\" --verify-live-package-handoff-bundle \"*) mode='verify';;\n  *) echo 'unexpected handoff-bundle mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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
