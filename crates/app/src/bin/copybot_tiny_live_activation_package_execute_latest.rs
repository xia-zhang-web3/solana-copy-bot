use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::package_turn_green_execute_frozen;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_execute_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-execute | --render-latest-execute-script | --run-latest-execute | --verify-latest-execute)";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PACKAGE_FILE_PREFIX: &str = "tiny_live_activation_package_";
const SESSION_FILE_SUFFIX: &str = ".session.json";
const STATUS_FILE_SUFFIX: &str = ".status.json";
const EXECUTE_LATEST_SESSION_VERSION: &str = "1";
const EXECUTE_LATEST_STATUS_VERSION: &str = "1";
const EXECUTE_FROZEN_BIN: &str = "copybot_tiny_live_activation_package_execute_frozen";
const CLERESTORY_BIN: &str = "copybot_tiny_live_activation_package_clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-execute handoff stays planning-safe by itself: it binds persisted immutable package truth to the existing frozen execution contract, but it never authorizes activation and never overrides Stage 3 or pre-activation refusal semantics";

const PACKAGE_CHAIN_STEPS: &[&str] = &[
    "turn_green",
    "execute_frozen",
    "decision_packet",
    "handoff_bundle",
    "review_receipt",
    "activation_ticket",
    "release_capsule",
    "attestation_seal",
    "provenance_certificate",
    "notarization_receipt",
    "registry_entry",
    "filing_certificate",
    "archive_receipt",
    "closure_certificate",
    "finality_receipt",
    "consummation_record",
    "completion_certificate",
    "culmination_receipt",
    "summit_certificate",
    "pinnacle_receipt",
    "capstone_certificate",
    "keystone_receipt",
    "cornerstone_certificate",
    "foundation_receipt",
    "bedrock_certificate",
    "basal_receipt",
    "substructure_certificate",
    "plinth_receipt",
    "pedestal_certificate",
    "dais_receipt",
    "rostrum_certificate",
    "podium_receipt",
    "lectern_certificate",
    "pulpit_receipt",
    "chancel_certificate",
    "apse_receipt",
    "sanctuary_certificate",
    "nave_receipt",
    "transept_certificate",
    "choir_receipt",
    "clerestory_certificate",
];

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
    PlanLatestExecute,
    RenderLatestExecuteScript,
    RunLatestExecute,
    VerifyLatestExecute,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageExecuteLatestVerdict {
    TinyLivePackageExecuteLatestPlanReady,
    TinyLivePackageExecuteLatestScriptRendered,
    TinyLivePackageExecuteLatestRefusedByMissingLatestChain,
    TinyLivePackageExecuteLatestRefusedByInvalidLatestChain,
    TinyLivePackageExecuteLatestRefusedByDriftedFrozenContract,
    TinyLivePackageExecuteLatestRefusedNowByStage3,
    TinyLivePackageExecuteLatestRefusedNowByPreActivationGate,
    TinyLivePackageExecuteLatestCompletedKeepRunning,
    TinyLivePackageExecuteLatestCompletedWithRollback,
    TinyLivePackageExecuteLatestCompletedBackupFailed,
    TinyLivePackageExecuteLatestCompletedApplyFailed,
    TinyLivePackageExecuteLatestCompletedWatchFailed,
    TinyLivePackageExecuteLatestVerifyOk,
    TinyLivePackageExecuteLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageExecuteLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedFrozenContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    CompletedKeepRunning,
    CompletedWithRollback,
    CompletedBackupFailed,
    CompletedApplyFailed,
    CompletedWatchFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_execute_frozen_session_dir: String,
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
    verify_latest_chain_command_summary: Option<String>,
    verify_historical_execute_frozen_command_summary: Option<String>,
    downstream_execute_frozen_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    root: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    latest_chain_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    nested_execute_frozen_session_dir: String,
    result: LivePackageExecuteLatestResult,
    reason: String,
    latest_chain_verdict: Option<String>,
    historical_execute_frozen_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_chain_step: Option<PackageExecuteLatestStepArtifact>,
    historical_execute_frozen_step: Option<PackageExecuteLatestStepArtifact>,
    execute_frozen_step: Option<PackageExecuteLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageExecuteLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_chain_report_path: PathBuf,
    historical_execute_frozen_report_path: PathBuf,
    execute_frozen_report_path: PathBuf,
    nested_execute_frozen_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageExecuteLatestVerdict,
    reason: String,
    root: Option<String>,
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
    session_dir: Option<String>,
    latest_execute_session_path: Option<String>,
    latest_execute_status_path: Option<String>,
    latest_execute_report_path: Option<String>,
    latest_chain_report_path: Option<String>,
    historical_execute_frozen_report_path: Option<String>,
    nested_execute_frozen_report_path: Option<String>,
    nested_execute_frozen_session_dir: Option<String>,
    latest_chain_verdict: Option<String>,
    historical_execute_frozen_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verify_latest_chain_command_summary: Option<String>,
    verify_historical_execute_frozen_command_summary: Option<String>,
    downstream_execute_frozen_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    execution_happened: bool,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestChainSnapshot {
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
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedLatestChain {
    snapshot: LatestChainSnapshot,
    latest_chain_report_json: Value,
    latest_chain_verdict: String,
    latest_chain_reason: String,
    latest_chain_generated_at: DateTime<Utc>,
    historical_execute_frozen_report_json: Value,
    historical_execute_frozen_verdict: String,
    historical_execute_frozen_reason: String,
    historical_execute_frozen_generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedFrozenContract,
}

#[derive(Debug, Clone)]
struct LatestChainResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    candidate_step_name: Option<String>,
    candidate_session_dir: Option<PathBuf>,
    snapshot: Option<LatestChainSnapshot>,
    latest_chain_report_json: Option<Value>,
    latest_chain_verdict: Option<String>,
    latest_chain_reason: Option<String>,
    latest_chain_generated_at: Option<DateTime<Utc>>,
    historical_execute_frozen_report_json: Option<Value>,
    historical_execute_frozen_verdict: Option<String>,
    historical_execute_frozen_reason: Option<String>,
    historical_execute_frozen_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct PackageSessionArtifact {
    package_step_name: String,
    step_rank: usize,
    session_dir: PathBuf,
    generated_at: Option<DateTime<Utc>>,
    file_modified_at: Option<DateTime<Utc>>,
    session_json: Option<Value>,
    status_json: Option<Value>,
    invalid_reasons: Vec<String>,
}

impl PackageSessionArtifact {
    fn ordering_time(&self) -> DateTime<Utc> {
        self.generated_at
            .or(self.file_modified_at)
            .unwrap_or_else(|| DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH))
    }
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
            "--plan-latest-execute" => set_mode(&mut mode, Mode::PlanLatestExecute, arg.as_str())?,
            "--render-latest-execute-script" => {
                set_mode(&mut mode, Mode::RenderLatestExecuteScript, arg.as_str())?
            }
            "--run-latest-execute" => set_mode(&mut mode, Mode::RunLatestExecute, arg.as_str())?,
            "--verify-latest-execute" => {
                set_mode(&mut mode, Mode::VerifyLatestExecute, arg.as_str())?
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
        Mode::PlanLatestExecute | Mode::RenderLatestExecuteScript | Mode::RunLatestExecute
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestExecuteScript) && output_path.is_none() {
        bail!("missing --output for --render-latest-execute-script");
    }
    if matches!(mode, Mode::RunLatestExecute | Mode::VerifyLatestExecute) && session_dir.is_none() {
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
        Mode::PlanLatestExecute => plan_latest_execute_report(&config)?,
        Mode::RenderLatestExecuteScript => render_latest_execute_script_report(&config)?,
        Mode::RunLatestExecute => run_latest_execute_report(&config)?,
        Mode::VerifyLatestExecute => verify_latest_execute_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_execute_report(config: &Config) -> Result<PackageExecuteLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_execute_latest.session"));
    let paths = execute_latest_paths(&suggested_session_dir);

    match resolve_latest_chain(root) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_execute",
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact turn_green {} and historical execute_frozen {} lineage for the accepted frozen execution contract",
                resolved.snapshot.latest_top_step_name,
                root.display(),
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
            "plan_latest_execute",
            root,
            Some(&suggested_session_dir),
            Some(&paths),
            &failure,
            false,
        )),
    }
}

fn render_latest_execute_script_report(config: &Config) -> Result<PackageExecuteLatestReport> {
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
    let paths = execute_latest_paths(&suggested_session_dir);

    match resolve_latest_chain(root) {
        Ok(resolved) => {
            let script = format!(
                "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --plan-live-package-execute-frozen\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --verify-live-package-execute-frozen\n",
                shell_escape_path(&resolved.snapshot.turn_green_session_dir),
                shell_escape_path(&resolved.snapshot.turn_green_session_dir),
                shell_escape_path(&paths.nested_execute_frozen_session_dir),
                shell_escape_path(&resolved.snapshot.turn_green_session_dir),
                shell_escape_path(&paths.nested_execute_frozen_session_dir),
            );
            write_script(output_path, &script)?;
            Ok(plan_report_from_resolution(
                "render_latest_execute_script",
                TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestScriptRendered,
                format!(
                    "rendered latest-execute handoff script to {} using the exact accepted execute_frozen contract",
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
            "render_latest_execute_script",
            root,
            Some(&suggested_session_dir),
            Some(&paths),
            &failure,
            false,
        )),
    }
}

fn run_latest_execute_report(config: &Config) -> Result<PackageExecuteLatestReport> {
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
            "refusing to overwrite existing latest-execute session dir {}",
            session_dir.display()
        );
    }
    let paths = execute_latest_paths(session_dir);
    let resolution = resolve_latest_chain(root);

    match resolution {
        Ok(resolved) => {
            package_turn_green_execute_frozen::validate_turn_green_session_dir(
                &resolved.snapshot.install_root,
                session_dir,
            )?;
            create_clean_session_dir(session_dir, "latest-execute")?;
            let session = planned_session(root, session_dir, &paths, Some(&resolved), None);
            persist_json(&paths.session_path, &session)?;
            persist_json(
                &paths.latest_chain_report_path,
                &resolved.latest_chain_report_json,
            )?;
            persist_json(
                &paths.historical_execute_frozen_report_path,
                &resolved.historical_execute_frozen_report_json,
            )?;
            let latest_chain_step = Some(step_artifact(
                &paths.latest_chain_report_path,
                "verify_live_package_clerestory_certificate",
                &resolved.latest_chain_verdict,
                &resolved.latest_chain_reason,
                resolved.latest_chain_generated_at,
            ));
            let historical_execute_frozen_step = Some(step_artifact(
                &paths.historical_execute_frozen_report_path,
                "verify_live_package_execute_frozen",
                &resolved.historical_execute_frozen_verdict,
                &resolved.historical_execute_frozen_reason,
                resolved.historical_execute_frozen_generated_at,
            ));

            let nested_run_raw = match run_json_command(
                EXECUTE_FROZEN_BIN,
                &[
                    "--turn-green-session-dir".to_string(),
                    resolved
                        .snapshot
                        .turn_green_session_dir
                        .display()
                        .to_string(),
                    "--session-dir".to_string(),
                    paths
                        .nested_execute_frozen_session_dir
                        .display()
                        .to_string(),
                    "--run-live-package-execute-frozen".to_string(),
                    "--json".to_string(),
                ],
            ) {
                Ok(value) => value,
                Err(error) => {
                    let status = refusal_status(
                        root,
                        session_dir,
                        &paths,
                        Some(&resolved),
                        LivePackageExecuteLatestResult::RefusedByDriftedFrozenContract,
                        format!(
                            "failed running downstream execute_frozen contract for {}: {error}",
                            resolved.snapshot.turn_green_session_dir.display()
                        ),
                        latest_chain_step,
                        historical_execute_frozen_step,
                        None,
                        resolved.snapshot.current_pre_activation_gate_verdict.clone(),
                        resolved.snapshot.current_pre_activation_gate_reason.clone(),
                        "the latest immutable chain resolved, but the downstream execute_frozen wrapper did not produce coherent persisted output".to_string(),
                    );
                    persist_json(&paths.status_path, &status)?;
                    let report = report_from_status(
                        "run_latest_execute",
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
            persist_json(&paths.execute_frozen_report_path, &nested_run_raw)?;
            let nested_verdict =
                required_string_from_value(&nested_run_raw, "verdict", "execute_frozen verdict")?;
            let nested_reason =
                required_string_from_value(&nested_run_raw, "reason", "execute_frozen reason")?;
            let nested_generated_at = required_datetime_from_value(
                &nested_run_raw,
                "generated_at",
                "execute_frozen generated_at",
            )?;
            let nested_step = Some(step_artifact(
                &paths.execute_frozen_report_path,
                "run_live_package_execute_frozen",
                &nested_verdict,
                &nested_reason,
                nested_generated_at,
            ));
            let (result, verdict) = map_execute_frozen_outcome(&nested_run_raw);
            let current_pre_activation_gate_verdict = extract_non_empty_string(
                Some(&nested_run_raw),
                "current_pre_activation_gate_verdict",
            );
            let current_pre_activation_gate_reason = extract_non_empty_string(
                Some(&nested_run_raw),
                "current_pre_activation_gate_reason",
            );
            let status = PackageExecuteLatestStatus {
                status_version: EXECUTE_LATEST_STATUS_VERSION.to_string(),
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
                nested_execute_frozen_session_dir: paths
                    .nested_execute_frozen_session_dir
                    .display()
                    .to_string(),
                result,
                reason: nested_reason,
                latest_chain_verdict: Some(resolved.latest_chain_verdict.clone()),
                historical_execute_frozen_verdict: Some(
                    resolved.historical_execute_frozen_verdict.clone(),
                ),
                current_pre_activation_gate_verdict,
                current_pre_activation_gate_reason,
                latest_chain_step,
                historical_execute_frozen_step,
                execute_frozen_step: nested_step,
                operator_next_action_summary: operator_next_action_summary(result),
                activation_authorized: false,
                explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_execute",
                verdict,
                session_dir,
                &paths,
                &session,
                &status,
                Vec::new(),
                status.execute_frozen_step.is_some(),
            );
            persist_json(&paths.report_path, &report)?;
            Ok(report)
        }
        Err(failure) => {
            create_clean_session_dir(session_dir, "latest-execute")?;
            let session = planned_session(root, session_dir, &paths, None, Some(&failure));
            persist_json(&paths.session_path, &session)?;
            if let Some(value) = &failure.latest_chain_report_json {
                persist_json(&paths.latest_chain_report_path, value)?;
            }
            if let Some(value) = &failure.historical_execute_frozen_report_json {
                persist_json(&paths.historical_execute_frozen_report_path, value)?;
            }
            let status =
                refusal_status_from_resolution_failure(root, session_dir, &paths, &failure);
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_execute",
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

fn verify_latest_execute_report(config: &Config) -> Result<PackageExecuteLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = execute_latest_paths(session_dir);
    let mut mismatches = Vec::new();

    let session: PackageExecuteLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-execute session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ))
        }
    };
    let status: PackageExecuteLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-execute status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ))
        }
    };
    let stored_report: PackageExecuteLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-execute report artifact {}: {error}",
                    paths.report_path.display()
                )],
            ))
        }
    };

    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-execute session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-execute status session_dir",
        &mut mismatches,
    );
    compare_string(
        stored_report.session_dir.as_deref().unwrap_or(""),
        &session_dir.display().to_string(),
        "latest-execute report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_execute_frozen_session_dir,
        &paths
            .nested_execute_frozen_session_dir
            .display()
            .to_string(),
        "latest-execute nested_execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_execute_frozen_session_dir,
        &paths
            .nested_execute_frozen_session_dir
            .display()
            .to_string(),
        "latest-execute status nested_execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_execute_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-execute report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_execute_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-execute report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_execute_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-execute report report path",
        &mut mismatches,
    );
    if status.activation_authorized {
        mismatches
            .push("latest-execute status must never mark activation_authorized=true".to_string());
    }
    if stored_report.activation_authorized {
        mismatches
            .push("latest-execute report must never mark activation_authorized=true".to_string());
    }

    let root = PathBuf::from(&session.root);
    let resolved = match resolve_latest_chain(&root) {
        Ok(value) => value,
        Err(failure) => {
            mismatches.push(format!(
                "failed resolving current latest immutable chain under {} during verify: {}",
                root.display(),
                failure.reason
            ));
            return Ok(verify_report_from_artifacts(
                session_dir,
                &paths,
                &session,
                &status,
                &stored_report,
                mismatches,
            ));
        }
    };

    compare_option_string(
        session.latest_top_step_name.as_deref(),
        Some(resolved.snapshot.latest_top_step_name.as_str()),
        "stored latest_top_step_name",
        &mut mismatches,
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
        "stored latest_top_session_dir",
        &mut mismatches,
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
        "stored turn_green_session_dir",
        &mut mismatches,
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
        "stored latest_chain_execute_frozen_session_dir",
        &mut mismatches,
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
        "stored decision_packet_session_dir",
        &mut mismatches,
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
        "stored choir_receipt_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        session.downstream_execute_frozen_command_summary.as_deref(),
        Some(
            downstream_execute_frozen_command_summary(
                &resolved.snapshot.turn_green_session_dir,
                &paths.nested_execute_frozen_session_dir,
            )
            .as_str(),
        ),
        "stored downstream_execute_frozen_command_summary",
        &mut mismatches,
    );
    compare_option_string(
        session.verify_latest_chain_command_summary.as_deref(),
        Some(
            verify_latest_chain_command_summary(
                &resolved.snapshot.choir_receipt_session_dir,
                &resolved.snapshot.decision_packet_session_dir,
                &resolved.snapshot.latest_top_session_dir,
            )
            .as_str(),
        ),
        "stored verify_latest_chain_command_summary",
        &mut mismatches,
    );
    compare_option_string(
        session
            .verify_historical_execute_frozen_command_summary
            .as_deref(),
        Some(
            verify_execute_frozen_command_summary(
                &resolved.snapshot.turn_green_session_dir,
                &resolved.snapshot.latest_chain_execute_frozen_session_dir,
            )
            .as_str(),
        ),
        "stored verify_historical_execute_frozen_command_summary",
        &mut mismatches,
    );
    compare_session_contract_against_snapshot(&session, &resolved.snapshot, &mut mismatches);
    compare_report_contract_against_snapshot(&stored_report, &resolved.snapshot, &mut mismatches);

    compare_step_path(
        &status.latest_chain_step,
        &paths.latest_chain_report_path,
        "latest_chain_step",
        &mut mismatches,
    );
    compare_step_path(
        &status.historical_execute_frozen_step,
        &paths.historical_execute_frozen_report_path,
        "historical_execute_frozen_step",
        &mut mismatches,
    );
    let _: Value = load_required_step_json(
        &status.latest_chain_step,
        &paths.latest_chain_report_path,
        "latest_chain_step",
        &mut mismatches,
    )?;
    let _: Value = load_required_step_json(
        &status.historical_execute_frozen_step,
        &paths.historical_execute_frozen_report_path,
        "historical_execute_frozen_step",
        &mut mismatches,
    )?;
    compare_option_string(
        stored_report.latest_chain_report_path.as_deref(),
        Some(
            paths
                .latest_chain_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_chain_report_path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .historical_execute_frozen_report_path
            .as_deref(),
        Some(
            paths
                .historical_execute_frozen_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report historical_execute_frozen_report_path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_chain_verdict.as_deref(),
        Some(resolved.latest_chain_verdict.as_str()),
        "stored report latest_chain_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.historical_execute_frozen_verdict.as_deref(),
        Some(resolved.historical_execute_frozen_verdict.as_str()),
        "stored report historical_execute_frozen_verdict",
        &mut mismatches,
    );

    if let Some(step) = &status.execute_frozen_step {
        compare_string(
            &step.path,
            &paths.execute_frozen_report_path.display().to_string(),
            "execute_frozen_step path",
            &mut mismatches,
        );
        let _: Value = load_json(&paths.execute_frozen_report_path).with_context(|| {
            format!(
                "failed reading {}",
                paths.execute_frozen_report_path.display()
            )
        })?;
        compare_option_string(
            stored_report.nested_execute_frozen_report_path.as_deref(),
            Some(
                paths
                    .execute_frozen_report_path
                    .display()
                    .to_string()
                    .as_str(),
            ),
            "stored report nested_execute_frozen_report_path",
            &mut mismatches,
        );

        let nested_verify_raw = match run_json_command(
            EXECUTE_FROZEN_BIN,
            &[
                "--turn-green-session-dir".to_string(),
                resolved
                    .snapshot
                    .turn_green_session_dir
                    .display()
                    .to_string(),
                "--session-dir".to_string(),
                paths
                    .nested_execute_frozen_session_dir
                    .display()
                    .to_string(),
                "--verify-live-package-execute-frozen".to_string(),
                "--json".to_string(),
            ],
        ) {
            Ok(value) => value,
            Err(error) => {
                mismatches.push(format!(
                    "failed verifying nested execute_frozen session under {}: {error}",
                    paths.nested_execute_frozen_session_dir.display()
                ));
                return Ok(verify_report_from_artifacts(
                    session_dir,
                    &paths,
                    &session,
                    &status,
                    &stored_report,
                    mismatches,
                ));
            }
        };
        let nested_verdict = required_string_from_value(
            &nested_verify_raw,
            "verdict",
            "nested execute_frozen verdict",
        )?;
        if nested_verdict != "tiny_live_package_execute_frozen_verify_ok" {
            mismatches.push(format!(
                "nested execute_frozen verification is non-green: {}",
                required_string_from_value(
                    &nested_verify_raw,
                    "reason",
                    "nested execute_frozen reason",
                )?
            ));
        }
        let (expected_result, _) = map_execute_frozen_outcome(&nested_verify_raw);
        if status.result != expected_result {
            mismatches.push(format!(
                "stored latest-execute result {} does not match nested execute_frozen verify {}",
                serialize_enum(&status.result),
                serialize_enum(&expected_result)
            ));
        }
        compare_option_string(
            stored_report.result.as_deref(),
            Some(serialize_enum(&status.result).as_str()),
            "stored report result",
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
    } else if matches!(
        status.result,
        LivePackageExecuteLatestResult::CompletedKeepRunning
            | LivePackageExecuteLatestResult::CompletedWithRollback
            | LivePackageExecuteLatestResult::CompletedBackupFailed
            | LivePackageExecuteLatestResult::CompletedApplyFailed
            | LivePackageExecuteLatestResult::CompletedWatchFailed
            | LivePackageExecuteLatestResult::RefusedNowByStage3
            | LivePackageExecuteLatestResult::RefusedNowByPreActivationGate
    ) {
        mismatches.push(
            "latest-execute status is missing execute_frozen_step even though downstream execute_frozen should have run"
                .to_string(),
        );
    }

    Ok(verify_report_from_artifacts(
        session_dir,
        &paths,
        &session,
        &status,
        &stored_report,
        mismatches,
    ))
}

fn resolve_latest_chain(
    root: &Path,
) -> std::result::Result<ResolvedLatestChain, LatestChainResolutionFailure> {
    let sessions = match scan_package_sessions(root) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestChainResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: error.to_string(),
                candidate_step_name: None,
                candidate_session_dir: None,
                snapshot: None,
                latest_chain_report_json: None,
                latest_chain_verdict: None,
                latest_chain_reason: None,
                latest_chain_generated_at: None,
                historical_execute_frozen_report_json: None,
                historical_execute_frozen_verdict: None,
                historical_execute_frozen_reason: None,
                historical_execute_frozen_generated_at: None,
            });
        }
    };
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::MissingLatestChain,
            reason: format!(
                "no persisted immutable tiny-live package chain exists under {}",
                root.display()
            ),
            candidate_step_name: None,
            candidate_session_dir: None,
            snapshot: None,
            latest_chain_report_json: None,
            latest_chain_verdict: None,
            latest_chain_reason: None,
            latest_chain_generated_at: None,
            historical_execute_frozen_report_json: None,
            historical_execute_frozen_verdict: None,
            historical_execute_frozen_reason: None,
            historical_execute_frozen_generated_at: None,
        });
    };

    if candidate.package_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: format!(
                "latest persisted top chain under {} is {} at {}, which is below {}",
                root.display(),
                candidate.package_step_name,
                candidate.session_dir.display(),
                TOP_ACCEPTED_PACKAGE_STEP
            ),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: None,
            latest_chain_report_json: None,
            latest_chain_verdict: None,
            latest_chain_reason: None,
            latest_chain_generated_at: None,
            historical_execute_frozen_report_json: None,
            historical_execute_frozen_verdict: None,
            historical_execute_frozen_reason: None,
            historical_execute_frozen_generated_at: None,
        });
    }

    let snapshot = match parse_latest_chain_snapshot(candidate) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestChainResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: error.to_string(),
                candidate_step_name: Some(candidate.package_step_name.clone()),
                candidate_session_dir: Some(candidate.session_dir.clone()),
                snapshot: None,
                latest_chain_report_json: None,
                latest_chain_verdict: None,
                latest_chain_reason: None,
                latest_chain_generated_at: None,
                historical_execute_frozen_report_json: None,
                historical_execute_frozen_verdict: None,
                historical_execute_frozen_reason: None,
                historical_execute_frozen_generated_at: None,
            });
        }
    };

    let latest_chain_raw = match run_json_command(
        CLERESTORY_BIN,
        &[
            "--choir-receipt-session-dir".to_string(),
            snapshot.choir_receipt_session_dir.display().to_string(),
            "--confirm-decision-packet-session-dir".to_string(),
            snapshot.decision_packet_session_dir.display().to_string(),
            "--session-dir".to_string(),
            snapshot.latest_top_session_dir.display().to_string(),
            "--verify-live-package-clerestory-certificate".to_string(),
            "--json".to_string(),
        ],
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestChainResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed verifying latest {} chain under {}: {error}",
                    TOP_ACCEPTED_PACKAGE_STEP,
                    snapshot.latest_top_session_dir.display()
                ),
                candidate_step_name: Some(candidate.package_step_name.clone()),
                candidate_session_dir: Some(candidate.session_dir.clone()),
                snapshot: Some(snapshot),
                latest_chain_report_json: None,
                latest_chain_verdict: None,
                latest_chain_reason: None,
                latest_chain_generated_at: None,
                historical_execute_frozen_report_json: None,
                historical_execute_frozen_verdict: None,
                historical_execute_frozen_reason: None,
                historical_execute_frozen_generated_at: None,
            });
        }
    };
    let latest_chain_verdict =
        match required_string_from_value(&latest_chain_raw, "verdict", "clerestory verify verdict")
        {
            Ok(value) => value,
            Err(error) => {
                return Err(LatestChainResolutionFailure {
                    kind: ResolutionFailureKind::InvalidLatestChain,
                    reason: error.to_string(),
                    candidate_step_name: Some(candidate.package_step_name.clone()),
                    candidate_session_dir: Some(candidate.session_dir.clone()),
                    snapshot: Some(snapshot),
                    latest_chain_report_json: Some(latest_chain_raw),
                    latest_chain_verdict: None,
                    latest_chain_reason: None,
                    latest_chain_generated_at: None,
                    historical_execute_frozen_report_json: None,
                    historical_execute_frozen_verdict: None,
                    historical_execute_frozen_reason: None,
                    historical_execute_frozen_generated_at: None,
                });
            }
        };
    let latest_chain_reason =
        required_string_from_value(&latest_chain_raw, "reason", "clerestory verify reason")
            .unwrap_or_else(|_| "latest clerestory verify did not return a reason".to_string());
    let latest_chain_generated_at =
        extract_datetime(Some(&latest_chain_raw), "generated_at").unwrap_or_else(Utc::now);
    let snapshot_after_clerestory = snapshot.clone();
    if latest_chain_verdict != "tiny_live_package_clerestory_certificate_verify_ok" {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: format!(
                "latest {} chain verification is non-green: {}",
                TOP_ACCEPTED_PACKAGE_STEP, latest_chain_reason
            ),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: Some(snapshot_after_clerestory),
            latest_chain_report_json: Some(latest_chain_raw),
            latest_chain_verdict: Some(latest_chain_verdict),
            latest_chain_reason: Some(latest_chain_reason),
            latest_chain_generated_at: Some(latest_chain_generated_at),
            historical_execute_frozen_report_json: None,
            historical_execute_frozen_verdict: None,
            historical_execute_frozen_reason: None,
            historical_execute_frozen_generated_at: None,
        });
    }
    let latest_chain_mismatches =
        compare_snapshot_against_clerestory_report(&snapshot_after_clerestory, &latest_chain_raw);
    if !latest_chain_mismatches.is_empty() {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: latest_chain_mismatches[0].clone(),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: Some(snapshot_after_clerestory.clone()),
            latest_chain_report_json: Some(latest_chain_raw),
            latest_chain_verdict: Some(latest_chain_verdict),
            latest_chain_reason: Some(latest_chain_reason),
            latest_chain_generated_at: Some(latest_chain_generated_at),
            historical_execute_frozen_report_json: None,
            historical_execute_frozen_verdict: None,
            historical_execute_frozen_reason: None,
            historical_execute_frozen_generated_at: None,
        });
    }

    let historical_execute_frozen_raw = match run_json_command(
        EXECUTE_FROZEN_BIN,
        &[
            "--turn-green-session-dir".to_string(),
            snapshot_after_clerestory
                .turn_green_session_dir
                .display()
                .to_string(),
            "--session-dir".to_string(),
            snapshot_after_clerestory
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string(),
            "--verify-live-package-execute-frozen".to_string(),
            "--json".to_string(),
        ],
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestChainResolutionFailure {
                kind: ResolutionFailureKind::DriftedFrozenContract,
                reason: format!(
                    "failed verifying historical execute_frozen lineage under {}: {error}",
                    snapshot_after_clerestory
                        .latest_chain_execute_frozen_session_dir
                        .display()
                ),
                candidate_step_name: Some(candidate.package_step_name.clone()),
                candidate_session_dir: Some(candidate.session_dir.clone()),
                snapshot: Some(snapshot_after_clerestory),
                latest_chain_report_json: Some(latest_chain_raw),
                latest_chain_verdict: Some(latest_chain_verdict),
                latest_chain_reason: Some(latest_chain_reason),
                latest_chain_generated_at: Some(latest_chain_generated_at),
                historical_execute_frozen_report_json: None,
                historical_execute_frozen_verdict: None,
                historical_execute_frozen_reason: None,
                historical_execute_frozen_generated_at: None,
            });
        }
    };
    let historical_execute_frozen_verdict = required_string_from_value(
        &historical_execute_frozen_raw,
        "verdict",
        "historical execute_frozen verify verdict",
    )
    .unwrap_or_else(|_| "tiny_live_package_execute_frozen_verify_invalid".to_string());
    let historical_execute_frozen_reason = required_string_from_value(
        &historical_execute_frozen_raw,
        "reason",
        "historical execute_frozen verify reason",
    )
    .unwrap_or_else(|_| "historical execute_frozen verify did not return a reason".to_string());
    let historical_execute_frozen_generated_at =
        extract_datetime(Some(&historical_execute_frozen_raw), "generated_at")
            .unwrap_or_else(Utc::now);
    if historical_execute_frozen_verdict != "tiny_live_package_execute_frozen_verify_ok" {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::DriftedFrozenContract,
            reason: format!(
                "historical execute_frozen lineage is non-green: {}",
                historical_execute_frozen_reason
            ),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: Some(snapshot_after_clerestory.clone()),
            latest_chain_report_json: Some(latest_chain_raw),
            latest_chain_verdict: Some(latest_chain_verdict),
            latest_chain_reason: Some(latest_chain_reason),
            latest_chain_generated_at: Some(latest_chain_generated_at),
            historical_execute_frozen_report_json: Some(historical_execute_frozen_raw),
            historical_execute_frozen_verdict: Some(historical_execute_frozen_verdict),
            historical_execute_frozen_reason: Some(historical_execute_frozen_reason),
            historical_execute_frozen_generated_at: Some(historical_execute_frozen_generated_at),
        });
    }
    let historical_mismatches = compare_snapshot_against_execute_frozen_verify(
        &snapshot_after_clerestory,
        &historical_execute_frozen_raw,
    );
    if !historical_mismatches.is_empty() {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::DriftedFrozenContract,
            reason: historical_mismatches[0].clone(),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: Some(snapshot_after_clerestory.clone()),
            latest_chain_report_json: Some(latest_chain_raw),
            latest_chain_verdict: Some(latest_chain_verdict),
            latest_chain_reason: Some(latest_chain_reason),
            latest_chain_generated_at: Some(latest_chain_generated_at),
            historical_execute_frozen_report_json: Some(historical_execute_frozen_raw),
            historical_execute_frozen_verdict: Some(historical_execute_frozen_verdict),
            historical_execute_frozen_reason: Some(historical_execute_frozen_reason),
            historical_execute_frozen_generated_at: Some(historical_execute_frozen_generated_at),
        });
    }

    let turn_green_contract = match package_turn_green_execute_frozen::load_live_package_turn_green_contract_for_execute_frozen(
        &snapshot_after_clerestory.turn_green_session_dir,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestChainResolutionFailure {
                kind: ResolutionFailureKind::DriftedFrozenContract,
                reason: format!(
                    "failed loading turn_green lineage under {}: {error}",
                    snapshot_after_clerestory.turn_green_session_dir.display()
                ),
                candidate_step_name: Some(candidate.package_step_name.clone()),
                candidate_session_dir: Some(candidate.session_dir.clone()),
                snapshot: Some(snapshot_after_clerestory),
                latest_chain_report_json: Some(latest_chain_raw),
                latest_chain_verdict: Some(latest_chain_verdict),
                latest_chain_reason: Some(latest_chain_reason),
                latest_chain_generated_at: Some(latest_chain_generated_at),
                historical_execute_frozen_report_json: Some(historical_execute_frozen_raw),
                historical_execute_frozen_verdict: Some(historical_execute_frozen_verdict),
                historical_execute_frozen_reason: Some(historical_execute_frozen_reason),
                historical_execute_frozen_generated_at: Some(historical_execute_frozen_generated_at),
            });
        }
    };
    let turn_green_mismatches = compare_snapshot_against_turn_green_contract(
        &snapshot_after_clerestory,
        &turn_green_contract,
    );
    if !turn_green_mismatches.is_empty() {
        return Err(LatestChainResolutionFailure {
            kind: ResolutionFailureKind::DriftedFrozenContract,
            reason: turn_green_mismatches[0].clone(),
            candidate_step_name: Some(candidate.package_step_name.clone()),
            candidate_session_dir: Some(candidate.session_dir.clone()),
            snapshot: Some(snapshot_after_clerestory),
            latest_chain_report_json: Some(latest_chain_raw),
            latest_chain_verdict: Some(latest_chain_verdict),
            latest_chain_reason: Some(latest_chain_reason),
            latest_chain_generated_at: Some(latest_chain_generated_at),
            historical_execute_frozen_report_json: Some(historical_execute_frozen_raw),
            historical_execute_frozen_verdict: Some(historical_execute_frozen_verdict),
            historical_execute_frozen_reason: Some(historical_execute_frozen_reason),
            historical_execute_frozen_generated_at: Some(historical_execute_frozen_generated_at),
        });
    }

    Ok(ResolvedLatestChain {
        snapshot: snapshot_after_clerestory,
        latest_chain_report_json: latest_chain_raw,
        latest_chain_verdict,
        latest_chain_reason,
        latest_chain_generated_at,
        historical_execute_frozen_report_json: historical_execute_frozen_raw,
        historical_execute_frozen_verdict,
        historical_execute_frozen_reason,
        historical_execute_frozen_generated_at,
    })
}

fn parse_latest_chain_snapshot(candidate: &PackageSessionArtifact) -> Result<LatestChainSnapshot> {
    let session_json = candidate.session_json.as_ref().ok_or_else(|| {
        anyhow!(
            "latest {} session artifact is missing or unparsable",
            candidate.package_step_name
        )
    })?;
    let status_json = candidate.status_json.as_ref().ok_or_else(|| {
        anyhow!(
            "latest {} status artifact is missing or unparsable",
            candidate.package_step_name
        )
    })?;
    if !candidate.invalid_reasons.is_empty() {
        bail!("{}", candidate.invalid_reasons[0]);
    }

    let expected_session_dir = candidate.session_dir.display().to_string();
    compare_required_persisted_path(
        session_json,
        "session_dir",
        &expected_session_dir,
        "latest session session_dir",
    )?;
    compare_required_persisted_path(
        status_json,
        "session_dir",
        &expected_session_dir,
        "latest status session_dir",
    )?;

    Ok(LatestChainSnapshot {
        latest_top_step_name: candidate.package_step_name.clone(),
        latest_top_session_dir: candidate.session_dir.clone(),
        choir_receipt_session_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "choir_receipt_session_dir",
            &candidate.package_step_name,
        )?),
        decision_packet_session_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "decision_packet_session_dir",
            &candidate.package_step_name,
        )?),
        latest_chain_execute_frozen_session_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "execute_frozen_session_dir",
            &candidate.package_step_name,
        )?),
        turn_green_session_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "turn_green_session_dir",
            &candidate.package_step_name,
        )?),
        launch_packet_session_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "launch_packet_session_dir",
            &candidate.package_step_name,
        )?),
        package_dir: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "package_dir",
            &candidate.package_step_name,
        )?),
        install_root: PathBuf::from(required_duplicated_string_field(
            status_json,
            session_json,
            "install_root",
            &candidate.package_step_name,
        )?),
        target_service_name: required_duplicated_string_field(
            status_json,
            session_json,
            "target_service_name",
            &candidate.package_step_name,
        )?,
        backend_command: required_duplicated_string_field(
            status_json,
            session_json,
            "backend_command",
            &candidate.package_step_name,
        )?,
        wrapper_timeout_ms: required_duplicated_u64_field(
            status_json,
            session_json,
            "wrapper_timeout_ms",
            &candidate.package_step_name,
        )?,
        service_status_max_staleness_ms: required_duplicated_u64_field(
            status_json,
            session_json,
            "service_status_max_staleness_ms",
            &candidate.package_step_name,
        )?,
        reviewed_frozen_live_cutover_controller_command_summary: required_duplicated_string_field(
            status_json,
            session_json,
            "reviewed_frozen_live_cutover_controller_command_summary",
            &candidate.package_step_name,
        )?,
        clerestory_certificate_summary: required_duplicated_string_field(
            status_json,
            session_json,
            "clerestory_certificate_summary",
            &candidate.package_step_name,
        )?,
        clerestory_certificate_sha256: required_duplicated_string_field(
            status_json,
            session_json,
            "clerestory_certificate_sha256",
            &candidate.package_step_name,
        )?,
        clerestory_certificate_algorithm: required_duplicated_string_field(
            status_json,
            session_json,
            "clerestory_certificate_algorithm",
            &candidate.package_step_name,
        )?,
        execute_frozen_result: extract_string(
            Some(status_json),
            Some(session_json),
            "execute_frozen_result",
        ),
        current_pre_activation_gate_verdict: extract_string(
            Some(status_json),
            Some(session_json),
            "current_pre_activation_gate_verdict",
        ),
        current_pre_activation_gate_reason: extract_string(
            Some(status_json),
            Some(session_json),
            "current_pre_activation_gate_reason",
        ),
    })
}

fn compare_snapshot_against_clerestory_report(
    snapshot: &LatestChainSnapshot,
    report: &Value,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_required_value_string(
        report,
        "session_dir",
        &snapshot.latest_top_session_dir.display().to_string(),
        "clerestory verify session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "choir_receipt_session_dir",
        &snapshot.choir_receipt_session_dir.display().to_string(),
        "clerestory verify choir_receipt_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "decision_packet_session_dir",
        &snapshot.decision_packet_session_dir.display().to_string(),
        "clerestory verify decision_packet_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "execute_frozen_session_dir",
        &snapshot
            .latest_chain_execute_frozen_session_dir
            .display()
            .to_string(),
        "clerestory verify execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "turn_green_session_dir",
        &snapshot.turn_green_session_dir.display().to_string(),
        "clerestory verify turn_green_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "launch_packet_session_dir",
        &snapshot.launch_packet_session_dir.display().to_string(),
        "clerestory verify launch_packet_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "package_dir",
        &snapshot.package_dir.display().to_string(),
        "clerestory verify package_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "install_root",
        &snapshot.install_root.display().to_string(),
        "clerestory verify install_root",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "target_service_name",
        &snapshot.target_service_name,
        "clerestory verify target_service_name",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "backend_command",
        &snapshot.backend_command,
        "clerestory verify backend_command",
        &mut mismatches,
    );
    compare_required_value_u64(
        report,
        "wrapper_timeout_ms",
        snapshot.wrapper_timeout_ms,
        "clerestory verify wrapper_timeout_ms",
        &mut mismatches,
    );
    compare_required_value_u64(
        report,
        "service_status_max_staleness_ms",
        snapshot.service_status_max_staleness_ms,
        "clerestory verify service_status_max_staleness_ms",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "reviewed_frozen_live_cutover_controller_command_summary",
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "clerestory verify reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "clerestory_certificate_summary",
        &snapshot.clerestory_certificate_summary,
        "clerestory verify clerestory_certificate_summary",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "clerestory_certificate_sha256",
        &snapshot.clerestory_certificate_sha256,
        "clerestory verify clerestory_certificate_sha256",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "clerestory_certificate_algorithm",
        &snapshot.clerestory_certificate_algorithm,
        "clerestory verify clerestory_certificate_algorithm",
        &mut mismatches,
    );
    if let Some(expected) = snapshot.execute_frozen_result.as_deref() {
        compare_required_value_string(
            report,
            "execute_frozen_result",
            expected,
            "clerestory verify execute_frozen_result",
            &mut mismatches,
        );
    }
    mismatches
}

fn compare_snapshot_against_execute_frozen_verify(
    snapshot: &LatestChainSnapshot,
    report: &Value,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_required_value_string(
        report,
        "turn_green_session_dir",
        &snapshot.turn_green_session_dir.display().to_string(),
        "historical execute_frozen verify turn_green_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "session_dir",
        &snapshot
            .latest_chain_execute_frozen_session_dir
            .display()
            .to_string(),
        "historical execute_frozen verify session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "launch_packet_session_dir",
        &snapshot.launch_packet_session_dir.display().to_string(),
        "historical execute_frozen verify launch_packet_session_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "package_dir",
        &snapshot.package_dir.display().to_string(),
        "historical execute_frozen verify package_dir",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "install_root",
        &snapshot.install_root.display().to_string(),
        "historical execute_frozen verify install_root",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "target_service_name",
        &snapshot.target_service_name,
        "historical execute_frozen verify target_service_name",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "backend_command",
        &snapshot.backend_command,
        "historical execute_frozen verify backend_command",
        &mut mismatches,
    );
    compare_required_value_u64(
        report,
        "wrapper_timeout_ms",
        snapshot.wrapper_timeout_ms,
        "historical execute_frozen verify wrapper_timeout_ms",
        &mut mismatches,
    );
    compare_required_value_u64(
        report,
        "service_status_max_staleness_ms",
        snapshot.service_status_max_staleness_ms,
        "historical execute_frozen verify service_status_max_staleness_ms",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "frozen_live_cutover_controller_command_summary",
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "historical execute_frozen verify frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_required_value_string(
        report,
        "execute_frozen_command_summary",
        &run_execute_frozen_command_summary(
            &snapshot.turn_green_session_dir,
            &snapshot.latest_chain_execute_frozen_session_dir,
        ),
        "historical execute_frozen verify execute_frozen_command_summary",
        &mut mismatches,
    );
    if let Some(expected) = snapshot.execute_frozen_result.as_deref() {
        compare_required_value_string(
            report,
            "result",
            expected,
            "historical execute_frozen verify result",
            &mut mismatches,
        );
    }
    mismatches
}

fn compare_snapshot_against_turn_green_contract(
    snapshot: &LatestChainSnapshot,
    contract: &package_turn_green_execute_frozen::LivePackageTurnGreenContractView,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    if contract.turn_green_session_dir != snapshot.turn_green_session_dir {
        mismatches.push(format!(
            "turn_green session dir drifted: stored {} expected {}",
            contract.turn_green_session_dir.display(),
            snapshot.turn_green_session_dir.display()
        ));
    }
    if contract.launch_packet_session_dir != snapshot.launch_packet_session_dir {
        mismatches.push(format!(
            "turn_green launch_packet_session_dir drifted: stored {} expected {}",
            contract.launch_packet_session_dir.display(),
            snapshot.launch_packet_session_dir.display()
        ));
    }
    if contract.package_dir != snapshot.package_dir {
        mismatches.push(format!(
            "turn_green package_dir drifted: stored {} expected {}",
            contract.package_dir.display(),
            snapshot.package_dir.display()
        ));
    }
    if contract.install_root != snapshot.install_root {
        mismatches.push(format!(
            "turn_green install_root drifted: stored {} expected {}",
            contract.install_root.display(),
            snapshot.install_root.display()
        ));
    }
    if contract.target_service_name != snapshot.target_service_name {
        mismatches.push(format!(
            "turn_green target_service_name drifted: stored {:?} expected {:?}",
            contract.target_service_name, snapshot.target_service_name
        ));
    }
    if contract.backend_command != snapshot.backend_command {
        mismatches.push(format!(
            "turn_green backend_command drifted: stored {:?} expected {:?}",
            contract.backend_command, snapshot.backend_command
        ));
    }
    if contract.wrapper_timeout_ms != snapshot.wrapper_timeout_ms {
        mismatches.push(format!(
            "turn_green wrapper_timeout_ms drifted: stored {} expected {}",
            contract.wrapper_timeout_ms, snapshot.wrapper_timeout_ms
        ));
    }
    if contract.service_status_max_staleness_ms != snapshot.service_status_max_staleness_ms {
        mismatches.push(format!(
            "turn_green service_status_max_staleness_ms drifted: stored {} expected {}",
            contract.service_status_max_staleness_ms, snapshot.service_status_max_staleness_ms
        ));
    }
    if contract.frozen_live_cutover_controller_command_summary
        != snapshot.reviewed_frozen_live_cutover_controller_command_summary
    {
        mismatches.push(format!(
            "turn_green frozen_live_cutover_controller_command_summary drifted: stored {:?} expected {:?}",
            contract.frozen_live_cutover_controller_command_summary,
            snapshot.reviewed_frozen_live_cutover_controller_command_summary
        ));
    }
    if contract.current_live_cutover_controller_command_summary
        != snapshot.reviewed_frozen_live_cutover_controller_command_summary
    {
        mismatches.push(format!(
            "turn_green current_live_cutover_controller_command_summary drifted: stored {:?} expected {:?}",
            contract.current_live_cutover_controller_command_summary,
            snapshot.reviewed_frozen_live_cutover_controller_command_summary
        ));
    }
    if !contract.frozen_controller_still_current {
        mismatches.push(
            "turn_green contract proves the frozen live cutover controller is no longer current"
                .to_string(),
        );
    }
    mismatches
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageExecuteLatestPaths,
    resolved: Option<&ResolvedLatestChain>,
    failure: Option<&LatestChainResolutionFailure>,
) -> PackageExecuteLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageExecuteLatestSession {
        session_version: EXECUTE_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_execute_frozen_session_dir: paths
            .nested_execute_frozen_session_dir
            .display()
            .to_string(),
        latest_top_step_name: snapshot
            .map(|value| value.latest_top_step_name.clone())
            .or_else(|| failure.and_then(|value| value.candidate_step_name.clone())),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string())
            .or_else(|| {
                failure.and_then(|value| {
                    value
                        .candidate_session_dir
                        .as_ref()
                        .map(|path| path.display().to_string())
                })
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
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        verify_latest_chain_command_summary: snapshot.map(|value| {
            verify_latest_chain_command_summary(
                &value.choir_receipt_session_dir,
                &value.decision_packet_session_dir,
                &value.latest_top_session_dir,
            )
        }),
        verify_historical_execute_frozen_command_summary: snapshot.map(|value| {
            verify_execute_frozen_command_summary(
                &value.turn_green_session_dir,
                &value.latest_chain_execute_frozen_session_dir,
            )
        }),
        downstream_execute_frozen_command_summary: snapshot.map(|value| {
            downstream_execute_frozen_command_summary(
                &value.turn_green_session_dir,
                &paths.nested_execute_frozen_session_dir,
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
    paths: &PackageExecuteLatestPaths,
    failure: &LatestChainResolutionFailure,
) -> PackageExecuteLatestStatus {
    let snapshot = failure.snapshot.as_ref();
    PackageExecuteLatestStatus {
        status_version: EXECUTE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        root: root.display().to_string(),
        latest_top_step_name: snapshot
            .map(|value| value.latest_top_step_name.clone())
            .or_else(|| failure.candidate_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string())
            .or_else(|| {
                failure
                    .candidate_session_dir
                    .as_ref()
                    .map(|path| path.display().to_string())
            }),
        latest_chain_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        nested_execute_frozen_session_dir: paths
            .nested_execute_frozen_session_dir
            .display()
            .to_string(),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_chain_verdict: failure.latest_chain_verdict.clone(),
        historical_execute_frozen_verdict: failure.historical_execute_frozen_verdict.clone(),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        latest_chain_step: failure.latest_chain_report_json.as_ref().map(|_| {
            step_artifact(
                &paths.latest_chain_report_path,
                "verify_live_package_clerestory_certificate",
                failure
                    .latest_chain_verdict
                    .as_deref()
                    .unwrap_or("tiny_live_package_clerestory_certificate_verify_invalid"),
                failure
                    .latest_chain_reason
                    .as_deref()
                    .unwrap_or(failure.reason.as_str()),
                failure.latest_chain_generated_at.unwrap_or_else(Utc::now),
            )
        }),
        historical_execute_frozen_step: failure.historical_execute_frozen_report_json.as_ref().map(
            |_| {
                step_artifact(
                    &paths.historical_execute_frozen_report_path,
                    "verify_live_package_execute_frozen",
                    failure
                        .historical_execute_frozen_verdict
                        .as_deref()
                        .unwrap_or("tiny_live_package_execute_frozen_verify_invalid"),
                    failure
                        .historical_execute_frozen_reason
                        .as_deref()
                        .unwrap_or(failure.reason.as_str()),
                    failure
                        .historical_execute_frozen_generated_at
                        .unwrap_or_else(Utc::now),
                )
            },
        ),
        execute_frozen_step: None,
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
    paths: &PackageExecuteLatestPaths,
    resolved: Option<&ResolvedLatestChain>,
    result: LivePackageExecuteLatestResult,
    reason: String,
    latest_chain_step: Option<PackageExecuteLatestStepArtifact>,
    historical_execute_frozen_step: Option<PackageExecuteLatestStepArtifact>,
    execute_frozen_step: Option<PackageExecuteLatestStepArtifact>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: String,
) -> PackageExecuteLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageExecuteLatestStatus {
        status_version: EXECUTE_LATEST_STATUS_VERSION.to_string(),
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
        nested_execute_frozen_session_dir: paths
            .nested_execute_frozen_session_dir
            .display()
            .to_string(),
        result,
        reason,
        latest_chain_verdict: resolved.map(|value| value.latest_chain_verdict.clone()),
        historical_execute_frozen_verdict: resolved
            .map(|value| value.historical_execute_frozen_verdict.clone()),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        latest_chain_step,
        historical_execute_frozen_step,
        execute_frozen_step,
        operator_next_action_summary,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageExecuteLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageExecuteLatestPaths,
    resolved: &ResolvedLatestChain,
    execution_happened: bool,
) -> PackageExecuteLatestReport {
    PackageExecuteLatestReport {
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
        package_dir: Some(resolved.snapshot.package_dir.display().to_string()),
        install_root: Some(resolved.snapshot.install_root.display().to_string()),
        target_service_name: Some(resolved.snapshot.target_service_name.clone()),
        backend_command: Some(resolved.snapshot.backend_command.clone()),
        wrapper_timeout_ms: Some(resolved.snapshot.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(resolved.snapshot.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        latest_execute_session_path: Some(paths.session_path.display().to_string()),
        latest_execute_status_path: Some(paths.status_path.display().to_string()),
        latest_execute_report_path: Some(paths.report_path.display().to_string()),
        latest_chain_report_path: Some(paths.latest_chain_report_path.display().to_string()),
        historical_execute_frozen_report_path: Some(
            paths
                .historical_execute_frozen_report_path
                .display()
                .to_string(),
        ),
        nested_execute_frozen_report_path: Some(
            paths.execute_frozen_report_path.display().to_string(),
        ),
        nested_execute_frozen_session_dir: Some(
            paths
                .nested_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        latest_chain_verdict: Some(resolved.latest_chain_verdict.clone()),
        historical_execute_frozen_verdict: Some(resolved.historical_execute_frozen_verdict.clone()),
        result: None,
        current_pre_activation_gate_verdict: resolved
            .snapshot
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: resolved
            .snapshot
            .current_pre_activation_gate_reason
            .clone(),
        verify_latest_chain_command_summary: Some(verify_latest_chain_command_summary(
            &resolved.snapshot.choir_receipt_session_dir,
            &resolved.snapshot.decision_packet_session_dir,
            &resolved.snapshot.latest_top_session_dir,
        )),
        verify_historical_execute_frozen_command_summary: Some(
            verify_execute_frozen_command_summary(
                &resolved.snapshot.turn_green_session_dir,
                &resolved.snapshot.latest_chain_execute_frozen_session_dir,
            ),
        ),
        downstream_execute_frozen_command_summary: Some(downstream_execute_frozen_command_summary(
            &resolved.snapshot.turn_green_session_dir,
            &paths.nested_execute_frozen_session_dir,
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
        execution_happened,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    _config: &Config,
    mode: &str,
    root: &Path,
    session_dir: Option<&Path>,
    paths: Option<&PackageExecuteLatestPaths>,
    failure: &LatestChainResolutionFailure,
    execution_happened: bool,
) -> PackageExecuteLatestReport {
    let snapshot = failure.snapshot.as_ref();
    let fallback_nested =
        session_dir.map(|dir| execute_latest_paths(dir).nested_execute_frozen_session_dir);
    PackageExecuteLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot
            .map(|value| value.latest_top_step_name.clone())
            .or_else(|| failure.candidate_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string())
            .or_else(|| {
                failure
                    .candidate_session_dir
                    .as_ref()
                    .map(|path| path.display().to_string())
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
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: session_dir.map(|value| value.display().to_string()),
        latest_execute_session_path: paths.map(|value| value.session_path.display().to_string()),
        latest_execute_status_path: paths.map(|value| value.status_path.display().to_string()),
        latest_execute_report_path: paths.map(|value| value.report_path.display().to_string()),
        latest_chain_report_path: paths
            .filter(|_| failure.latest_chain_report_json.is_some())
            .map(|value| value.latest_chain_report_path.display().to_string()),
        historical_execute_frozen_report_path: paths
            .filter(|_| failure.historical_execute_frozen_report_json.is_some())
            .map(|value| {
                value
                    .historical_execute_frozen_report_path
                    .display()
                    .to_string()
            }),
        nested_execute_frozen_report_path: paths
            .map(|value| value.execute_frozen_report_path.display().to_string()),
        nested_execute_frozen_session_dir: paths
            .map(|value| {
                value
                    .nested_execute_frozen_session_dir
                    .display()
                    .to_string()
            })
            .or_else(|| {
                fallback_nested
                    .as_ref()
                    .map(|path| path.display().to_string())
            }),
        latest_chain_verdict: failure.latest_chain_verdict.clone(),
        historical_execute_frozen_verdict: failure.historical_execute_frozen_verdict.clone(),
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        verify_latest_chain_command_summary: snapshot.map(|value| {
            verify_latest_chain_command_summary(
                &value.choir_receipt_session_dir,
                &value.decision_packet_session_dir,
                &value.latest_top_session_dir,
            )
        }),
        verify_historical_execute_frozen_command_summary: snapshot.map(|value| {
            verify_execute_frozen_command_summary(
                &value.turn_green_session_dir,
                &value.latest_chain_execute_frozen_session_dir,
            )
        }),
        downstream_execute_frozen_command_summary: match (snapshot, paths) {
            (Some(value), Some(paths)) => Some(downstream_execute_frozen_command_summary(
                &value.turn_green_session_dir,
                &paths.nested_execute_frozen_session_dir,
            )),
            _ => None,
        },
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
        execution_happened,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageExecuteLatestVerdict,
    session_dir: &Path,
    paths: &PackageExecuteLatestPaths,
    session: &PackageExecuteLatestSession,
    status: &PackageExecuteLatestStatus,
    verification_mismatches: Vec<String>,
    execution_happened: bool,
) -> PackageExecuteLatestReport {
    PackageExecuteLatestReport {
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
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        latest_execute_session_path: Some(paths.session_path.display().to_string()),
        latest_execute_status_path: Some(paths.status_path.display().to_string()),
        latest_execute_report_path: Some(paths.report_path.display().to_string()),
        latest_chain_report_path: status
            .latest_chain_step
            .as_ref()
            .map(|_| paths.latest_chain_report_path.display().to_string()),
        historical_execute_frozen_report_path: status.historical_execute_frozen_step.as_ref().map(
            |_| {
                paths
                    .historical_execute_frozen_report_path
                    .display()
                    .to_string()
            },
        ),
        nested_execute_frozen_report_path: status
            .execute_frozen_step
            .as_ref()
            .map(|_| paths.execute_frozen_report_path.display().to_string()),
        nested_execute_frozen_session_dir: Some(
            paths
                .nested_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        latest_chain_verdict: status.latest_chain_verdict.clone(),
        historical_execute_frozen_verdict: status.historical_execute_frozen_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verify_latest_chain_command_summary: session.verify_latest_chain_command_summary.clone(),
        verify_historical_execute_frozen_command_summary: session
            .verify_historical_execute_frozen_command_summary
            .clone(),
        downstream_execute_frozen_command_summary: session
            .downstream_execute_frozen_command_summary
            .clone(),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        execution_happened,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_report_without_session(
    session_dir: &Path,
    mismatches: Vec<String>,
) -> PackageExecuteLatestReport {
    let reason = mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-execute verification failed".to_string());
    PackageExecuteLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_execute".to_string(),
        verdict: TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid,
        reason,
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        choir_receipt_session_dir: None,
        decision_packet_session_dir: None,
        latest_chain_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        latest_execute_session_path: None,
        latest_execute_status_path: None,
        latest_execute_report_path: None,
        latest_chain_report_path: None,
        historical_execute_frozen_report_path: None,
        nested_execute_frozen_report_path: None,
        nested_execute_frozen_session_dir: None,
        latest_chain_verdict: None,
        historical_execute_frozen_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verify_latest_chain_command_summary: None,
        verify_historical_execute_frozen_command_summary: None,
        downstream_execute_frozen_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches: mismatches,
        execution_happened: false,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_report_from_artifacts(
    session_dir: &Path,
    paths: &PackageExecuteLatestPaths,
    session: &PackageExecuteLatestSession,
    status: &PackageExecuteLatestStatus,
    _stored_report: &PackageExecuteLatestReport,
    mismatches: Vec<String>,
) -> PackageExecuteLatestReport {
    let verdict = if mismatches.is_empty() {
        TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyOk
    } else {
        TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-execute session under {} coherently binds the current latest immutable package chain to the accepted frozen execute_frozen contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-execute verification failed".to_string())
    };
    report_from_status(
        "verify_latest_execute",
        verdict,
        session_dir,
        paths,
        session,
        status,
        mismatches,
        status.execute_frozen_step.is_some(),
    )
    .with_reason(reason)
}

trait ReportExt {
    fn with_reason(self, reason: String) -> Self;
}

impl ReportExt for PackageExecuteLatestReport {
    fn with_reason(mut self, reason: String) -> Self {
        self.reason = reason;
        self
    }
}

fn verdict_from_failure_kind(kind: ResolutionFailureKind) -> TinyLivePackageExecuteLatestVerdict {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedFrozenContract => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByDriftedFrozenContract
        }
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageExecuteLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageExecuteLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageExecuteLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedFrozenContract => {
            LivePackageExecuteLatestResult::RefusedByDriftedFrozenContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageExecuteLatestResult,
) -> TinyLivePackageExecuteLatestVerdict {
    match result {
        LivePackageExecuteLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByMissingLatestChain
        }
        LivePackageExecuteLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByInvalidLatestChain
        }
        LivePackageExecuteLatestResult::RefusedByDriftedFrozenContract => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByDriftedFrozenContract
        }
        LivePackageExecuteLatestResult::RefusedNowByStage3 => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedNowByStage3
        }
        LivePackageExecuteLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedNowByPreActivationGate
        }
        LivePackageExecuteLatestResult::CompletedKeepRunning => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedKeepRunning
        }
        LivePackageExecuteLatestResult::CompletedWithRollback => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedWithRollback
        }
        LivePackageExecuteLatestResult::CompletedBackupFailed => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedBackupFailed
        }
        LivePackageExecuteLatestResult::CompletedApplyFailed => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedApplyFailed
        }
        LivePackageExecuteLatestResult::CompletedWatchFailed => {
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedWatchFailed
        }
    }
}

fn map_execute_frozen_outcome(
    raw: &Value,
) -> (
    LivePackageExecuteLatestResult,
    TinyLivePackageExecuteLatestVerdict,
) {
    match extract_non_empty_string(Some(raw), "result").as_deref() {
        Some("completed_keep_running") => (
            LivePackageExecuteLatestResult::CompletedKeepRunning,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedKeepRunning,
        ),
        Some("completed_with_rollback") => (
            LivePackageExecuteLatestResult::CompletedWithRollback,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedWithRollback,
        ),
        Some("completed_backup_failed") => (
            LivePackageExecuteLatestResult::CompletedBackupFailed,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedBackupFailed,
        ),
        Some("completed_apply_failed") => (
            LivePackageExecuteLatestResult::CompletedApplyFailed,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedApplyFailed,
        ),
        Some("completed_watch_failed") => (
            LivePackageExecuteLatestResult::CompletedWatchFailed,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedWatchFailed,
        ),
        Some("refused_now_by_stage3") | Some("refused_by_stage3") => (
            LivePackageExecuteLatestResult::RefusedNowByStage3,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedNowByStage3,
        ),
        Some("refused_now_by_pre_activation_gate")
        | Some("refused_by_pre_activation_gate") => (
            LivePackageExecuteLatestResult::RefusedNowByPreActivationGate,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedNowByPreActivationGate,
        ),
        _ => (
            LivePackageExecuteLatestResult::RefusedByDriftedFrozenContract,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByDriftedFrozenContract,
        ),
    }
}

fn operator_next_action_summary(result: LivePackageExecuteLatestResult) -> String {
    match result {
        LivePackageExecuteLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not restitch turn_green or execute_frozen lineage by hand".to_string()
        }
        LivePackageExecuteLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or tampered; mint a fresh coherent top chain instead of bypassing the frozen path".to_string()
        }
        LivePackageExecuteLatestResult::RefusedByDriftedFrozenContract => {
            "the latest immutable chain no longer proves the exact frozen execute_frozen contract; reconcile lineage drift before any execution attempt".to_string()
        }
        LivePackageExecuteLatestResult::RefusedNowByStage3 => {
            "the handoff resolved cleanly, but Stage 3 truth is still non-green right now; rerun later without changing the frozen contract".to_string()
        }
        LivePackageExecuteLatestResult::RefusedNowByPreActivationGate => {
            "the handoff resolved cleanly, but the pre-activation gate is still non-green right now; rerun later without changing the frozen contract".to_string()
        }
        LivePackageExecuteLatestResult::CompletedKeepRunning => {
            "the latest immutable chain executed through the accepted frozen contract and the target kept running".to_string()
        }
        LivePackageExecuteLatestResult::CompletedWithRollback => {
            "the latest immutable chain reached the frozen contract and rolled back cleanly; inspect nested execute_frozen evidence before any retry".to_string()
        }
        LivePackageExecuteLatestResult::CompletedBackupFailed => {
            "the frozen contract stopped before apply because backup creation failed; inspect nested execute_frozen evidence".to_string()
        }
        LivePackageExecuteLatestResult::CompletedApplyFailed => {
            "the frozen contract reached apply and failed; inspect nested execute_frozen evidence before any retry".to_string()
        }
        LivePackageExecuteLatestResult::CompletedWatchFailed => {
            "the frozen contract reached watch and degraded; inspect nested execute_frozen evidence before any retry".to_string()
        }
    }
}

fn execute_latest_paths(session_dir: &Path) -> PackageExecuteLatestPaths {
    PackageExecuteLatestPaths {
        session_path: session_dir.join("tiny_live_activation_package_execute_latest.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_execute_latest.status.json"),
        report_path: session_dir.join("tiny_live_activation_package_execute_latest.report.json"),
        latest_chain_report_path: session_dir
            .join("tiny_live_activation_package_execute_latest.latest_chain.report.json"),
        historical_execute_frozen_report_path: session_dir.join(
            "tiny_live_activation_package_execute_latest.historical_execute_frozen.report.json",
        ),
        execute_frozen_report_path: session_dir
            .join("tiny_live_activation_package_execute_latest.execute_frozen.report.json"),
        nested_execute_frozen_session_dir: session_dir
            .join("tiny_live_activation_package_execute_latest.execute_frozen_session"),
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

fn verify_latest_chain_command_summary(
    choir_receipt_session_dir: &Path,
    decision_packet_session_dir: &Path,
    latest_top_session_dir: &Path,
) -> String {
    format!(
        "{CLERESTORY_BIN} --choir-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-clerestory-certificate",
        shell_escape_path(choir_receipt_session_dir),
        shell_escape_path(decision_packet_session_dir),
        shell_escape_path(latest_top_session_dir),
    )
}

fn verify_execute_frozen_command_summary(
    turn_green_session_dir: &Path,
    execute_frozen_session_dir: &Path,
) -> String {
    format!(
        "{EXECUTE_FROZEN_BIN} --turn-green-session-dir {} --session-dir {} --verify-live-package-execute-frozen",
        shell_escape_path(turn_green_session_dir),
        shell_escape_path(execute_frozen_session_dir),
    )
}

fn run_execute_frozen_command_summary(
    turn_green_session_dir: &Path,
    execute_frozen_session_dir: &Path,
) -> String {
    format!(
        "{EXECUTE_FROZEN_BIN} --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen",
        shell_escape_path(turn_green_session_dir),
        shell_escape_path(execute_frozen_session_dir),
    )
}

fn downstream_execute_frozen_command_summary(
    turn_green_session_dir: &Path,
    execute_frozen_session_dir: &Path,
) -> String {
    run_execute_frozen_command_summary(turn_green_session_dir, execute_frozen_session_dir)
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)
        .with_context(|| format!("failed writing latest-execute script to {}", path.display()))?;
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
) -> PackageExecuteLatestStepArtifact {
    PackageExecuteLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageExecuteLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-execute status"))?;
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn compare_step_path(
    step: &Option<PackageExecuteLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if let Some(step) = step {
        compare_string(
            &step.path,
            &expected_path.display().to_string(),
            &format!("{label} path"),
            mismatches,
        );
    }
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

fn compare_session_contract_against_snapshot(
    session: &PackageExecuteLatestSession,
    snapshot: &LatestChainSnapshot,
    mismatches: &mut Vec<String>,
) {
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
        session.choir_receipt_session_dir.as_deref(),
        Some(
            snapshot
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
            snapshot
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
            snapshot
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
            snapshot
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
            snapshot
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
}

fn compare_report_contract_against_snapshot(
    report: &PackageExecuteLatestReport,
    snapshot: &LatestChainSnapshot,
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
        report.choir_receipt_session_dir.as_deref(),
        Some(
            snapshot
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
            snapshot
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
            snapshot
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
            snapshot
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
            snapshot
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
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageExecuteLatestReport) -> String {
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
    if let Some(value) = &report.turn_green_session_dir {
        lines.push(format!("turn_green_session_dir={value}"));
    }
    if let Some(value) = &report.latest_chain_execute_frozen_session_dir {
        lines.push(format!("latest_chain_execute_frozen_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!("execution_happened={}", report.execution_happened));
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_reason {
        lines.push(format!("current_pre_activation_gate_reason={value}"));
    }
    if let Some(value) = &report.downstream_execute_frozen_command_summary {
        lines.push(format!("downstream_execute_frozen_command_summary={value}"));
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

fn scan_package_sessions(root: &Path) -> Result<Vec<PackageSessionArtifact>> {
    if !root.exists() {
        bail!("root {} does not exist", root.display());
    }
    if !root.is_dir() {
        bail!("root {} is not a directory", root.display());
    }

    let mut session_paths = Vec::new();
    collect_session_paths(root, &mut session_paths)?;
    let mut sessions = Vec::new();
    for session_path in session_paths {
        if let Some(surface) = inspect_session_path(&session_path)? {
            sessions.push(surface);
        }
    }
    Ok(sessions)
}

fn collect_session_paths(dir: &Path, session_paths: &mut Vec<PathBuf>) -> Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("failed reading directory {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed reading file type for {}", path.display()))?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_session_paths(&path, session_paths)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if name.starts_with(PACKAGE_FILE_PREFIX) && name.ends_with(SESSION_FILE_SUFFIX) {
            session_paths.push(path);
        }
    }
    Ok(())
}

fn inspect_session_path(session_path: &Path) -> Result<Option<PackageSessionArtifact>> {
    let Some(file_name) = session_path.file_name().and_then(OsStr::to_str) else {
        return Ok(None);
    };
    let Some(step_name) = session_step_name_from_session_file(file_name) else {
        return Ok(None);
    };
    if step_rank(&step_name) == 0 {
        return Ok(None);
    }
    let session_dir = session_path.parent().ok_or_else(|| {
        anyhow!(
            "session path {} has no parent directory",
            session_path.display()
        )
    })?;
    Ok(Some(inspect_package_artifact(session_dir, &step_name)?))
}

fn inspect_package_artifact(session_dir: &Path, step_name: &str) -> Result<PackageSessionArtifact> {
    let session_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
    ));
    let status_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
    ));
    let session_json = load_json_value(&session_path).ok();
    let status_json = load_json_value(&status_path).ok();
    let mut invalid_reasons = Vec::new();
    if !session_path.exists() {
        invalid_reasons.push(format!(
            "missing session artifact {}",
            session_path.display()
        ));
    } else if session_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing session artifact {}",
            session_path.display()
        ));
    }
    if !status_path.exists() {
        invalid_reasons.push(format!("missing status artifact {}", status_path.display()));
    } else if status_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing status artifact {}",
            status_path.display()
        ));
    }
    Ok(PackageSessionArtifact {
        package_step_name: step_name.to_string(),
        step_rank: step_rank(step_name),
        session_dir: session_dir.to_path_buf(),
        generated_at: extract_datetime(status_json.as_ref(), "updated_at")
            .or_else(|| extract_datetime(session_json.as_ref(), "planned_at")),
        file_modified_at: file_modified_at(&session_path),
        session_json,
        status_json,
        invalid_reasons,
    })
}

fn select_latest_top_chain_candidate(
    sessions: &[PackageSessionArtifact],
) -> Option<&PackageSessionArtifact> {
    let max_rank = sessions.iter().map(|session| session.step_rank).max()?;
    sessions
        .iter()
        .filter(|session| session.step_rank == max_rank)
        .max_by(|left, right| {
            left.ordering_time()
                .cmp(&right.ordering_time())
                .then_with(|| left.session_dir.cmp(&right.session_dir))
        })
}

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    Some(DateTime::<Utc>::from(modified))
}

fn session_step_name_from_session_file(file_name: &str) -> Option<String> {
    let trimmed = file_name.strip_prefix(PACKAGE_FILE_PREFIX)?;
    let step = trimmed.strip_suffix(SESSION_FILE_SUFFIX)?;
    if step.is_empty() {
        None
    } else {
        Some(step.to_string())
    }
}

fn step_rank(step_name: &str) -> usize {
    PACKAGE_CHAIN_STEPS
        .iter()
        .position(|candidate| *candidate == step_name)
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn load_json_value(path: &Path) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn extract_datetime(value: Option<&Value>, key: &str) -> Option<DateTime<Utc>> {
    value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_string(status: Option<&Value>, session: Option<&Value>, key: &str) -> Option<String> {
    extract_non_empty_string(status, key).or_else(|| extract_non_empty_string(session, key))
}

fn extract_non_empty_string(value: Option<&Value>, key: &str) -> Option<String> {
    let candidate = value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|raw| !raw.is_empty())?;
    Some(candidate.to_string())
}

fn extract_non_empty_u64(value: Option<&Value>, key: &str) -> Option<u64> {
    value.and_then(|json| json.get(key)).and_then(Value::as_u64)
}

fn required_duplicated_string_field(
    status: &Value,
    session: &Value,
    key: &str,
    step_name: &str,
) -> Result<String> {
    let status_value = extract_non_empty_string(Some(status), key);
    let session_value = extract_non_empty_string(Some(session), key);
    match (status_value, session_value) {
        (Some(left), Some(right)) if left != right => bail!(
            "{step_name} persisted {key} drifted between session/status: {:?} vs {:?}",
            left,
            right
        ),
        (Some(value), Some(_)) | (Some(value), None) | (None, Some(value)) => Ok(value),
        (None, None) => bail!("{step_name} is missing required field {key}"),
    }
}

fn required_duplicated_u64_field(
    status: &Value,
    session: &Value,
    key: &str,
    step_name: &str,
) -> Result<u64> {
    let status_value = extract_non_empty_u64(Some(status), key);
    let session_value = extract_non_empty_u64(Some(session), key);
    match (status_value, session_value) {
        (Some(left), Some(right)) if left != right => bail!(
            "{step_name} persisted {key} drifted between session/status: {} vs {}",
            left,
            right
        ),
        (Some(value), Some(_)) | (Some(value), None) | (None, Some(value)) => Ok(value),
        (None, None) => bail!("{step_name} is missing required field {key}"),
    }
}

fn required_string_from_value(value: &Value, key: &str, label: &str) -> Result<String> {
    extract_non_empty_string(Some(value), key).ok_or_else(|| anyhow!("{label} is missing"))
}

fn required_datetime_from_value(value: &Value, key: &str, label: &str) -> Result<DateTime<Utc>> {
    extract_datetime(Some(value), key).ok_or_else(|| anyhow!("{label} is missing"))
}

fn compare_required_persisted_path(
    value: &Value,
    key: &str,
    expected: &str,
    label: &str,
) -> Result<()> {
    let actual = required_string_from_value(value, key, label)?;
    if actual != expected {
        bail!(
            "{label} {:?} does not match expected {:?}",
            actual,
            expected
        );
    }
    Ok(())
}

fn compare_required_value_string(
    value: &Value,
    key: &str,
    expected: &str,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    match extract_non_empty_string(Some(value), key) {
        Some(actual) if actual != expected => mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        )),
        Some(_) => {}
        None => mismatches.push(format!("{label} is missing")),
    }
}

fn compare_required_value_u64(
    value: &Value,
    key: &str,
    expected: u64,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    match extract_non_empty_u64(Some(value), key) {
        Some(actual) if actual != expected => mismatches.push(format!(
            "{label} {} does not match expected {}",
            actual, expected
        )),
        Some(_) => {}
        None => mismatches.push(format!("{label} is missing")),
    }
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
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_expected_turn_green_and_execute_frozen_lineage(
    ) {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_plan_valid",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let older = fixture.root.join("older-clerestory-session");
        fs::create_dir_all(&older).unwrap();
        write_clerestory_chain(
            &older,
            &fixture.choir_receipt_session_dir,
            &fixture.decision_packet_session_dir,
            &fixture.historical_execute_frozen_session_dir,
            &fixture.turn_green_session_dir,
            &fixture.launch_packet_session_dir,
            &fixture.package_dir,
            &fixture.install_root,
            &fixture.backend_command,
            "older clerestory summary",
            "older-clerestory-sha",
            Utc::now() - chrono::Duration::minutes(10),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_execute_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestPlanReady
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
        assert!(report
            .downstream_execute_frozen_command_summary
            .as_deref()
            .unwrap()
            .contains("copybot_tiny_live_activation_package_execute_frozen"));
    }

    #[test]
    fn render_mode_emits_exact_downstream_frozen_execution_contract_not_second_controller_path() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_render_script",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_execute_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains("copybot_tiny_live_activation_package_execute_frozen"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
        assert!(!script
            .contains("copybot_tiny_live_activation_package_execute_latest --run-latest-execute"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        let missing_fixture = fake_execute_latest_fixture(
            "execute_latest_missing_latest",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        fs::remove_file(
            missing_fixture
                .latest_top_session_dir
                .join("tiny_live_activation_package_clerestory_certificate.session.json"),
        )
        .unwrap();
        fs::remove_file(
            missing_fixture
                .latest_top_session_dir
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
        )
        .unwrap();
        let _path_guard = PathGuard::install(&missing_fixture.bin_dir);
        let missing = run_latest_execute_report(&missing_fixture.run_config()).unwrap();
        assert_eq!(
            missing.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByMissingLatestChain
        );

        let invalid_fixture = fake_execute_latest_fixture(
            "execute_latest_invalid_latest",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let session_path = invalid_fixture
            .latest_top_session_dir
            .join("tiny_live_activation_package_clerestory_certificate.session.json");
        let mut session_json: Value = load_json(&session_path).unwrap();
        session_json
            .as_object_mut()
            .unwrap()
            .remove("turn_green_session_dir");
        persist_json(&session_path, &session_json).unwrap();
        let invalid = run_latest_execute_report(&invalid_fixture.run_config()).unwrap();
        assert_eq!(
            invalid.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByInvalidLatestChain
        );

        let below_fixture = fake_execute_latest_fixture(
            "execute_latest_below_top",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        fs::remove_file(
            below_fixture
                .latest_top_session_dir
                .join("tiny_live_activation_package_clerestory_certificate.session.json"),
        )
        .unwrap();
        fs::remove_file(
            below_fixture
                .latest_top_session_dir
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
        )
        .unwrap();
        let below_session_dir = below_fixture.root.join("choir-only-session");
        fs::create_dir_all(&below_session_dir).unwrap();
        write_generic_step_surface(
            &below_session_dir,
            "choir_receipt",
            json!({
                "planned_at": (Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
                "session_dir": below_session_dir.display().to_string(),
                "choir_receipt_summary": "choir summary",
                "choir_receipt_sha256": "choir-sha",
                "choir_receipt_algorithm": "sha256",
                "turn_green_session_dir": below_fixture.turn_green_session_dir.display().to_string()
            }),
            json!({
                "updated_at": (Utc::now() + chrono::Duration::minutes(1)).to_rfc3339(),
                "session_dir": below_session_dir.display().to_string(),
                "result": "ready_for_manual_execution_when_gate_turns_green",
                "choir_receipt_summary": "choir summary",
                "choir_receipt_sha256": "choir-sha",
                "choir_receipt_algorithm": "sha256",
                "turn_green_session_dir": below_fixture.turn_green_session_dir.display().to_string()
            }),
        );
        let below = run_latest_execute_report(&below_fixture.run_config()).unwrap();
        assert_eq!(
            below.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByInvalidLatestChain
        );
    }

    #[test]
    fn run_mode_refuses_when_nested_frozen_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_drifted_frozen_contract",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let controller_path = fixture
            .turn_green_session_dir
            .join("tiny_live_activation_package_turn_green.current_controller.report.json");
        persist_json(
            &controller_path,
            &json!({
                "run_live_cutover_command_summary": "drifted current controller"
            }),
        )
        .unwrap();
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_execute_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedByDriftedFrozenContract
        );
        assert!(!report.execution_happened);
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_execute_artifacts_are_tampered() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_verify_tamper",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_latest_execute_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestCompletedKeepRunning
        );

        let paths = execute_latest_paths(&fixture.wrapper_session_dir);
        let mut session: PackageExecuteLatestSession = load_json(&paths.session_path).unwrap();
        session.downstream_execute_frozen_command_summary = Some("tampered command".to_string());
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_latest_execute_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("downstream_execute_frozen_command_summary")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_session_reviewed_frozen_controller_summary_drifts() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_verify_session_controller_tamper",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_execute_report(&fixture.run_config()).unwrap();

        let paths = execute_latest_paths(&fixture.wrapper_session_dir);
        let mut session: PackageExecuteLatestSession = load_json(&paths.session_path).unwrap();
        session.reviewed_frozen_live_cutover_controller_command_summary =
            Some("tampered frozen controller summary".to_string());
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_latest_execute_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains(
                    "stored session reviewed_frozen_live_cutover_controller_command_summary"
                )),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_report_clerestory_identity_drifts() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_verify_report_clerestory_tamper",
            Some(nested_execute_frozen_run_report("completed_keep_running")),
            Some(nested_execute_frozen_verify_report(
                "completed_keep_running",
            )),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_execute_report(&fixture.run_config()).unwrap();

        let paths = execute_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageExecuteLatestReport = load_json(&paths.report_path).unwrap();
        report.clerestory_certificate_sha256 = Some("tampered-clerestory-sha".to_string());
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_execute_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored report clerestory_certificate_sha256")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_installed_target_binding_fields_drift_in_session_or_report() {
        {
            let session_fixture = fake_execute_latest_fixture(
                "execute_latest_verify_session_package_dir_tamper",
                Some(nested_execute_frozen_run_report("completed_keep_running")),
                Some(nested_execute_frozen_verify_report(
                    "completed_keep_running",
                )),
            );
            let _path_guard = PathGuard::install(&session_fixture.bin_dir);
            run_latest_execute_report(&session_fixture.run_config()).unwrap();

            let session_paths = execute_latest_paths(&session_fixture.wrapper_session_dir);
            let mut session: PackageExecuteLatestSession =
                load_json(&session_paths.session_path).unwrap();
            session.package_dir = Some("/tampered/package-dir".to_string());
            persist_json(&session_paths.session_path, &session).unwrap();

            let session_verify =
                verify_latest_execute_report(&session_fixture.verify_config()).unwrap();
            assert_eq!(
                session_verify.verdict,
                TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
            );
            assert!(
                session_verify
                    .verification_mismatches
                    .iter()
                    .any(|entry| entry.contains("stored session package_dir")),
                "{:#?}",
                session_verify.verification_mismatches
            );
        }

        {
            let report_fixture = fake_execute_latest_fixture(
                "execute_latest_verify_report_install_root_tamper",
                Some(nested_execute_frozen_run_report("completed_keep_running")),
                Some(nested_execute_frozen_verify_report(
                    "completed_keep_running",
                )),
            );
            let _path_guard = PathGuard::install(&report_fixture.bin_dir);
            run_latest_execute_report(&report_fixture.run_config()).unwrap();

            let report_paths = execute_latest_paths(&report_fixture.wrapper_session_dir);
            let mut report: PackageExecuteLatestReport =
                load_json(&report_paths.report_path).unwrap();
            report.install_root = Some("/tampered/install-root".to_string());
            persist_json(&report_paths.report_path, &report).unwrap();

            let report_verify =
                verify_latest_execute_report(&report_fixture.verify_config()).unwrap();
            assert_eq!(
                report_verify.verdict,
                TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyInvalid
            );
            assert!(
                report_verify
                    .verification_mismatches
                    .iter()
                    .any(|entry| entry.contains("stored report install_root")),
                "{:#?}",
                report_verify.verification_mismatches
            );
        }
    }

    #[test]
    fn all_modes_preserve_stage3_refusal_semantics_and_do_not_imply_activation_authorization() {
        let fixture = fake_execute_latest_fixture(
            "execute_latest_stage3_refusal",
            Some(nested_execute_frozen_run_report("refused_now_by_stage3")),
            Some(nested_execute_frozen_verify_report("refused_now_by_stage3")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let plan = plan_latest_execute_report(&fixture.plan_config()).unwrap();
        assert!(!plan.activation_authorized);

        let run = run_latest_execute_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestRefusedNowByStage3
        );
        assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
        assert!(!run.activation_authorized);

        let verify = verify_latest_execute_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteLatestVerdict::TinyLivePackageExecuteLatestVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
        assert!(!verify.activation_authorized);
    }

    struct FakeExecuteLatestFixture {
        root: PathBuf,
        bin_dir: PathBuf,
        output_script_path: PathBuf,
        wrapper_session_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        turn_green_session_dir: PathBuf,
        historical_execute_frozen_session_dir: PathBuf,
        choir_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
    }

    impl FakeExecuteLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestExecute,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestExecuteScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestExecute,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestExecute,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_execute_latest_fixture(
        label: &str,
        nested_run_report: Option<Value>,
        nested_verify_report: Option<Value>,
    ) -> FakeExecuteLatestFixture {
        let fixture_dir = temp_dir(label);
        let root = fixture_dir.join("root");
        let bin_dir = fixture_dir.join("bin");
        let output_script_path = fixture_dir.join("execute-latest.sh");
        let wrapper_session_dir = fixture_dir.join("execute-latest-session");
        let latest_top_session_dir = root.join("latest-clerestory-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let historical_execute_frozen_session_dir =
            fixture_dir.join("historical-execute-frozen-session");
        let choir_receipt_session_dir = fixture_dir.join("choir-receipt-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("install-root");
        let backend_command = fixture_dir.join("backend");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&bin_dir).unwrap();
        fs::create_dir_all(&latest_top_session_dir).unwrap();
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&historical_execute_frozen_session_dir).unwrap();
        fs::create_dir_all(&choir_receipt_session_dir).unwrap();
        fs::create_dir_all(&decision_packet_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();
        write_turn_green_surface(
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
            &frozen_controller_command,
        );
        write_clerestory_chain(
            &latest_top_session_dir,
            &choir_receipt_session_dir,
            &decision_packet_session_dir,
            &historical_execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
            "latest clerestory summary",
            "latest-clerestory-sha",
            Utc::now(),
        );

        let clerestory_verify_path = fixture_dir.join("clerestory.verify.json");
        persist_json(
            &clerestory_verify_path,
            &clerestory_verify_report(
                &latest_top_session_dir,
                &choir_receipt_session_dir,
                &decision_packet_session_dir,
                &historical_execute_frozen_session_dir,
                &turn_green_session_dir,
                &launch_packet_session_dir,
                &package_dir,
                &install_root,
                &backend_command,
                &frozen_controller_command,
                "latest clerestory summary",
                "latest-clerestory-sha",
            ),
        )
        .unwrap();

        let historical_execute_frozen_verify_path =
            fixture_dir.join("historical.execute_frozen.verify.json");
        persist_json(
            &historical_execute_frozen_verify_path,
            &historical_execute_frozen_verify_report(
                &turn_green_session_dir,
                &historical_execute_frozen_session_dir,
                &launch_packet_session_dir,
                &package_dir,
                &install_root,
                &backend_command,
                &frozen_controller_command,
            ),
        )
        .unwrap();

        let nested_run_path = fixture_dir.join("nested.execute_frozen.run.json");
        let nested_verify_path = fixture_dir.join("nested.execute_frozen.verify.json");
        if let Some(report) = nested_run_report {
            persist_json(
                &nested_run_path,
                &bind_nested_execute_frozen_report(
                    report,
                    &turn_green_session_dir,
                    &wrapper_session_dir,
                    &launch_packet_session_dir,
                    &package_dir,
                    &install_root,
                    &backend_command,
                    &frozen_controller_command,
                ),
            )
            .unwrap();
        }
        if let Some(report) = nested_verify_report {
            persist_json(
                &nested_verify_path,
                &bind_nested_execute_frozen_report(
                    report,
                    &turn_green_session_dir,
                    &wrapper_session_dir,
                    &launch_packet_session_dir,
                    &package_dir,
                    &install_root,
                    &backend_command,
                    &frozen_controller_command,
                ),
            )
            .unwrap();
        }

        write_fake_clerestory_verify_command(
            &bin_dir.join(CLERESTORY_BIN),
            &clerestory_verify_path,
            &choir_receipt_session_dir,
            &decision_packet_session_dir,
            &latest_top_session_dir,
        );
        write_fake_execute_frozen_command(
            &bin_dir.join(EXECUTE_FROZEN_BIN),
            &historical_execute_frozen_verify_path,
            &nested_run_path,
            &nested_verify_path,
            &turn_green_session_dir,
            &historical_execute_frozen_session_dir,
            &wrapper_session_dir,
        );

        FakeExecuteLatestFixture {
            root,
            bin_dir,
            output_script_path,
            wrapper_session_dir,
            latest_top_session_dir,
            turn_green_session_dir,
            historical_execute_frozen_session_dir,
            choir_receipt_session_dir,
            decision_packet_session_dir,
            launch_packet_session_dir,
            package_dir,
            install_root,
            backend_command,
        }
    }

    fn write_turn_green_surface(
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
    ) {
        let frozen_launch_packet_report_path = turn_green_session_dir
            .join("tiny_live_activation_package_turn_green.frozen_launch_packet.report.json");
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
                "result": "refused_now_by_stage3",
                "executable_now": false,
                "frozen_controller_still_current": true,
                "frozen_launch_packet_step": {
                    "path": frozen_launch_packet_report_path.display().to_string()
                },
                "current_pre_activation_gate_verdict": "blocked",
                "current_pre_activation_gate_reason": "stage3 blocked",
                "operator_next_action_summary": "wait for stage3",
                "explicit_statement": "turn-green statement"
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
                "run_live_cutover_command_summary": frozen_controller_command
            }),
        )
        .unwrap();
    }

    fn write_clerestory_chain(
        session_dir: &Path,
        choir_receipt_session_dir: &Path,
        decision_packet_session_dir: &Path,
        execute_frozen_session_dir: &Path,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        summary: &str,
        sha: &str,
        timestamp: DateTime<Utc>,
    ) {
        let session_json = json!({
            "planned_at": timestamp.to_rfc3339(),
            "session_dir": session_dir.display().to_string(),
            "choir_receipt_session_dir": choir_receipt_session_dir.display().to_string(),
            "decision_packet_session_dir": decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
            "package_dir": package_dir.display().to_string(),
            "install_root": install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "reviewed_frozen_live_cutover_controller_command_summary": "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg",
            "clerestory_certificate_summary": summary,
            "clerestory_certificate_sha256": sha,
            "clerestory_certificate_algorithm": "sha256",
            "execute_frozen_result": "refused_now_by_stage3",
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked"
        });
        let status_json = json!({
            "updated_at": timestamp.to_rfc3339(),
            "session_dir": session_dir.display().to_string(),
            "result": "ready_for_manual_execution_when_gate_turns_green",
            "choir_receipt_session_dir": choir_receipt_session_dir.display().to_string(),
            "decision_packet_session_dir": decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
            "package_dir": package_dir.display().to_string(),
            "install_root": install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "reviewed_frozen_live_cutover_controller_command_summary": "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg",
            "clerestory_certificate_summary": summary,
            "clerestory_certificate_sha256": sha,
            "clerestory_certificate_algorithm": "sha256",
            "execute_frozen_result": "refused_now_by_stage3",
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked"
        });
        write_generic_step_surface(
            session_dir,
            "clerestory_certificate",
            session_json,
            status_json,
        );
    }

    fn write_generic_step_surface(
        session_dir: &Path,
        step_name: &str,
        session_json: Value,
        status_json: Value,
    ) {
        persist_json(
            &session_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
            )),
            &session_json,
        )
        .unwrap();
        persist_json(
            &session_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
            )),
            &status_json,
        )
        .unwrap();
    }

    fn clerestory_verify_report(
        latest_top_session_dir: &Path,
        choir_receipt_session_dir: &Path,
        decision_packet_session_dir: &Path,
        execute_frozen_session_dir: &Path,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
        summary: &str,
        sha: &str,
    ) -> Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_clerestory_certificate_verify_ok",
            "reason": "clerestory ok",
            "session_dir": latest_top_session_dir.display().to_string(),
            "choir_receipt_session_dir": choir_receipt_session_dir.display().to_string(),
            "decision_packet_session_dir": decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
            "package_dir": package_dir.display().to_string(),
            "install_root": install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "execute_frozen_result": "refused_now_by_stage3",
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked",
            "reviewed_frozen_live_cutover_controller_command_summary": frozen_controller_command,
            "clerestory_certificate_summary": summary,
            "clerestory_certificate_sha256": sha,
            "clerestory_certificate_algorithm": "sha256"
        })
    }

    fn historical_execute_frozen_verify_report(
        turn_green_session_dir: &Path,
        historical_execute_frozen_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
    ) -> Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_execute_frozen_verify_ok",
            "reason": "historical execute_frozen ok",
            "turn_green_session_dir": turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
            "package_dir": package_dir.display().to_string(),
            "install_root": install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": historical_execute_frozen_session_dir.display().to_string(),
            "execute_frozen_command_summary": run_execute_frozen_command_summary(
                turn_green_session_dir,
                historical_execute_frozen_session_dir,
            ),
            "frozen_live_cutover_controller_command_summary": frozen_controller_command,
            "result": "refused_now_by_stage3",
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked",
            "execution_happened": false
        })
    }

    fn nested_execute_frozen_run_report(result: &str) -> Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": match result {
                "completed_keep_running" => "tiny_live_package_execute_frozen_completed_keep_running",
                "completed_with_rollback" => "tiny_live_package_execute_frozen_completed_with_rollback",
                "refused_now_by_stage3" => "tiny_live_package_execute_frozen_refused_now_by_stage3",
                "refused_now_by_pre_activation_gate" => "tiny_live_package_execute_frozen_refused_now_by_pre_activation_gate",
                _ => "tiny_live_package_execute_frozen_refused_now_by_invalid_or_drifted_contract",
            },
            "reason": format!("nested execute_frozen {result}"),
            "result": result,
            "current_pre_activation_gate_verdict": if result == "refused_now_by_pre_activation_gate" { "blocked" } else { "green" },
            "current_pre_activation_gate_reason": if result == "refused_now_by_stage3" { "stage3 blocked" } else if result == "refused_now_by_pre_activation_gate" { "pre gate blocked" } else { "ok" },
            "execution_happened": matches!(result, "completed_keep_running" | "completed_with_rollback")
        })
    }

    fn nested_execute_frozen_verify_report(result: &str) -> Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_execute_frozen_verify_ok",
            "reason": format!("nested execute_frozen {result}"),
            "result": result,
            "current_pre_activation_gate_verdict": if result == "refused_now_by_pre_activation_gate" { "blocked" } else { "green" },
            "current_pre_activation_gate_reason": if result == "refused_now_by_stage3" { "stage3 blocked" } else if result == "refused_now_by_pre_activation_gate" { "pre gate blocked" } else { "ok" },
            "execution_happened": matches!(result, "completed_keep_running" | "completed_with_rollback")
        })
    }

    fn bind_nested_execute_frozen_report(
        mut report: Value,
        turn_green_session_dir: &Path,
        wrapper_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
    ) -> Value {
        let nested_session_dir =
            execute_latest_paths(wrapper_session_dir).nested_execute_frozen_session_dir;
        let object = report.as_object_mut().unwrap();
        object.insert(
            "turn_green_session_dir".to_string(),
            json!(turn_green_session_dir.display().to_string()),
        );
        object.insert(
            "launch_packet_session_dir".to_string(),
            json!(launch_packet_session_dir.display().to_string()),
        );
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
            "session_dir".to_string(),
            json!(nested_session_dir.display().to_string()),
        );
        object.insert(
            "execute_frozen_command_summary".to_string(),
            json!(run_execute_frozen_command_summary(
                turn_green_session_dir,
                &nested_session_dir,
            )),
        );
        object.insert(
            "frozen_live_cutover_controller_command_summary".to_string(),
            json!(frozen_controller_command),
        );
        report
    }

    fn write_fake_clerestory_verify_command(
        path: &Path,
        json_path: &Path,
        choir_receipt_session_dir: &Path,
        decision_packet_session_dir: &Path,
        latest_top_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --choir-receipt-session-dir {} \"*) : ;;\n  *) echo 'unexpected choir-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected clerestory session-dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            choir_receipt_session_dir.display(),
            decision_packet_session_dir.display(),
            latest_top_session_dir.display(),
            json_path.display()
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_execute_frozen_command(
        path: &Path,
        historical_verify_json_path: &Path,
        nested_run_json_path: &Path,
        nested_verify_json_path: &Path,
        turn_green_session_dir: &Path,
        historical_execute_frozen_session_dir: &Path,
        wrapper_session_dir: &Path,
    ) {
        let nested_session_dir =
            execute_latest_paths(wrapper_session_dir).nested_execute_frozen_session_dir;
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --turn-green-session-dir {} \"*) : ;;\n  *) echo 'unexpected turn-green-session-dir' >&2; exit 1;;\nesac\nsession_kind=''\ncase \" $* \" in\n  *\" --session-dir {} \"*) session_kind='historical';;\n  *\" --session-dir {} \"*) session_kind='nested';;\n  *) echo 'unexpected execute_frozen session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-execute-frozen \"*) mode='run';;\n  *\" --verify-live-package-execute-frozen \"*) mode='verify';;\n  *) echo 'unexpected execute_frozen mode' >&2; exit 1;;\nesac\nif [ \"$session_kind\" = 'historical' ] && [ \"$mode\" = 'verify' ]; then\n  cat '{}'\nelif [ \"$session_kind\" = 'nested' ] && [ \"$mode\" = 'run' ]; then\n  cat '{}'\nelif [ \"$session_kind\" = 'nested' ] && [ \"$mode\" = 'verify' ]; then\n  cat '{}'\nelse\n  echo 'unexpected historical/nested execute_frozen mode combination' >&2\n  exit 1\nfi\n",
            turn_green_session_dir.display(),
            historical_execute_frozen_session_dir.display(),
            nested_session_dir.display(),
            historical_verify_json_path.display(),
            nested_run_json_path.display(),
            nested_verify_json_path.display(),
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
