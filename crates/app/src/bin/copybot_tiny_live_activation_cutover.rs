use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_config::load_from_path;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "../live_service_control_wrapper_contract.rs"]
pub(crate) mod live_service_control_wrapper_contract;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_execute.rs"]
pub(crate) mod tiny_live_activation_execute;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
pub(crate) mod tiny_live_activation_live_execute;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_watch.rs"]
pub(crate) mod tiny_live_activation_watch;

const USAGE: &str = "usage: copybot_tiny_live_activation_cutover --activation-config <path> --rollback-config <path> --runtime-dir <path> --target-config <path> --target-service <name> --service-control-command <path> --backup-dir <path> [--session-dir <path>] [--output <path>] [--json] [--startup-timeout-ms <ms>] [--rollback-timeout-ms <ms>] [--watch-window-seconds <seconds>] [--sample-cadence-ms <ms>] [--max-observation-staleness-ms <ms>] (--plan-cutover | --render-cutover-script | --verify-cutover-target | --plan-live-cutover | --run-live-cutover | --verify-cutover-session)";
const CUTOVER_SESSION_VERSION: &str = "1";
const CUTOVER_STATUS_VERSION: &str = "1";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_ROLLBACK_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_SAMPLE_CADENCE_MS: u64 = 1_000;
const MIN_SAMPLE_CADENCE_MS: u64 = 100;
const MIN_OBSERVATION_STALENESS_MS: u64 = 3_000;
const OBSERVATION_STALENESS_MULTIPLIER: u64 = 3;

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
    activation_config_path: PathBuf,
    rollback_config_path: PathBuf,
    runtime_dir: PathBuf,
    target_config_path: PathBuf,
    target_service_name: String,
    service_control_command_path: PathBuf,
    backup_dir: PathBuf,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanCutover,
    RenderCutoverScript,
    VerifyCutoverTarget,
    PlanLiveCutover,
    RunLiveCutover,
    VerifyCutoverSession,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveCutoverVerdict {
    TinyLiveCutoverPlanReady,
    TinyLiveCutoverScriptRendered,
    TinyLiveCutoverVerifyOk,
    TinyLiveCutoverVerifyInvalid,
    TinyLiveLiveCutoverPlanReady,
    TinyLiveLiveCutoverRefusedByStage3,
    TinyLiveLiveCutoverRefusedByPreActivationGate,
    TinyLiveLiveCutoverRefusedByInvalidTarget,
    TinyLiveLiveCutoverRefusedByMissingBackup,
    TinyLiveLiveCutoverCompletedKeepRunning,
    TinyLiveLiveCutoverCompletedWithRollback,
    TinyLiveLiveCutoverFailedBeforeApply,
    TinyLiveLiveCutoverFailedDuringWatch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LiveCutoverResult {
    CompletedKeepRunning,
    CompletedWithRollback,
    FailedBeforeApply,
    FailedDuringWatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    target_config_path: String,
    target_service_name: String,
    service_control_command_path: String,
    backup_dir: String,
    session_dir: String,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: u64,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: u64,
    backup_command_summary: Option<String>,
    verify_command_summary: Option<String>,
    apply_command_summary: Option<String>,
    watch_live_command_summary: Option<String>,
    rollback_command_summary: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    runtime_dir: String,
    result: LiveCutoverResult,
    reason: String,
    decision: String,
    bounded_but_degraded: bool,
    verify_target_step: Option<CutoverStepArtifact>,
    apply_step: Option<CutoverStepArtifact>,
    watch_step: Option<CutoverStepArtifact>,
    rollback_step: Option<CutoverStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct CutoverPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    verify_target_report_path: PathBuf,
    apply_report_path: PathBuf,
    watch_report_path: PathBuf,
    rollback_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CutoverReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveCutoverVerdict,
    reason: String,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    target_config_path: String,
    target_service_name: String,
    service_control_command_path: String,
    backup_dir: String,
    session_dir: Option<String>,
    watch_window_seconds: u64,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: u64,
    cutover_session_path: Option<String>,
    cutover_status_path: Option<String>,
    live_result: Option<String>,
    decision: Option<String>,
    bounded_but_degraded: bool,
    verify_target_step: Option<CutoverStepArtifact>,
    apply_step: Option<CutoverStepArtifact>,
    watch_step: Option<CutoverStepArtifact>,
    rollback_step: Option<CutoverStepArtifact>,
    backup_command_summary: Option<String>,
    verify_command_summary: Option<String>,
    apply_command_summary: Option<String>,
    watch_live_command_summary: Option<String>,
    rollback_command_summary: Option<String>,
    pre_activation_gate_verdict_used: Option<String>,
    pre_activation_gate_reason_used: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_plan_verdict_used: Option<String>,
    activation_plan_reason_used: Option<String>,
    guardrail_verdict_used: Option<String>,
    guardrail_reason_used: Option<String>,
    backup_proof_present: Option<bool>,
    service_control_wrapper_verification:
        Option<tiny_live_activation_live_execute::ServiceControlWrapperVerificationSummary>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PackageCutoverPlanSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) cutover_plan_command_summary: Option<String>,
    pub(crate) backup_command_summary: Option<String>,
    pub(crate) verify_command_summary: Option<String>,
    pub(crate) watch_live_command_summary: Option<String>,
    pub(crate) rollback_command_summary: Option<String>,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) verification_mismatches: Vec<String>,
    pub(crate) activation_authorized: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct CutoverRehearsalObservationSeed {
    pub(crate) target_running: bool,
    pub(crate) total_execution_attempts: u64,
    pub(crate) execution_error_count: u64,
    pub(crate) adapter_contract_failure_count: u64,
    pub(crate) policy_echo_mismatch_count: u64,
    pub(crate) fee_or_slippage_breach_count: u64,
    pub(crate) connectivity_degraded_seconds: u64,
    pub(crate) daily_realized_loss_sol: f64,
    pub(crate) consecutive_hard_failures: u32,
    pub(crate) observed_at: Option<DateTime<Utc>>,
    pub(crate) sample_window_started_at: Option<DateTime<Utc>>,
    pub(crate) note: String,
}

impl Default for CutoverRehearsalObservationSeed {
    fn default() -> Self {
        Self {
            target_running: true,
            total_execution_attempts: 12,
            execution_error_count: 0,
            adapter_contract_failure_count: 0,
            policy_echo_mismatch_count: 0,
            fee_or_slippage_breach_count: 0,
            connectivity_degraded_seconds: 0,
            daily_realized_loss_sol: 0.0,
            consecutive_hard_failures: 0,
            observed_at: None,
            sample_window_started_at: None,
            note: "shadow_cutover_rehearsal".to_string(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct CutoverRehearsalStep {
    pub(crate) report_json: serde_json::Value,
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) session_dir: Option<String>,
    pub(crate) cutover_session_path: Option<String>,
    pub(crate) cutover_status_path: Option<String>,
    pub(crate) live_result: Option<String>,
    pub(crate) decision: Option<String>,
    pub(crate) bounded_but_degraded: bool,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) verification_mismatches: Vec<String>,
    pub(crate) activation_authorized: bool,
}

#[derive(Debug, Clone)]
struct CutoverBundle {
    live_plan: tiny_live_activation_live_execute::LiveExecuteReport,
    live_verify: tiny_live_activation_live_execute::LiveExecuteReport,
    watch_plan: tiny_live_activation_watch::WatchReport,
    watch_window_seconds: u64,
    max_observation_staleness_ms: u64,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut activation_config_path: Option<PathBuf> = None;
    let mut rollback_config_path: Option<PathBuf> = None;
    let mut runtime_dir: Option<PathBuf> = None;
    let mut target_config_path: Option<PathBuf> = None;
    let mut target_service_name: Option<String> = None;
    let mut service_control_command_path: Option<PathBuf> = None;
    let mut backup_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut startup_timeout_ms = DEFAULT_STARTUP_TIMEOUT_MS;
    let mut rollback_timeout_ms = DEFAULT_ROLLBACK_TIMEOUT_MS;
    let mut watch_window_seconds: Option<u64> = None;
    let mut sample_cadence_ms = DEFAULT_SAMPLE_CADENCE_MS;
    let mut max_observation_staleness_ms: Option<u64> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--activation-config" => {
                activation_config_path = Some(PathBuf::from(parse_string_arg(
                    "--activation-config",
                    args.next(),
                )?))
            }
            "--rollback-config" => {
                rollback_config_path = Some(PathBuf::from(parse_string_arg(
                    "--rollback-config",
                    args.next(),
                )?))
            }
            "--runtime-dir" => {
                runtime_dir = Some(PathBuf::from(parse_string_arg(
                    "--runtime-dir",
                    args.next(),
                )?))
            }
            "--target-config" => {
                target_config_path = Some(PathBuf::from(parse_string_arg(
                    "--target-config",
                    args.next(),
                )?))
            }
            "--target-service" => {
                target_service_name = Some(parse_string_arg("--target-service", args.next())?)
            }
            "--service-control-command" => {
                service_control_command_path = Some(PathBuf::from(parse_string_arg(
                    "--service-control-command",
                    args.next(),
                )?))
            }
            "--backup-dir" => {
                backup_dir = Some(PathBuf::from(parse_string_arg(
                    "--backup-dir",
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
            "--startup-timeout-ms" => {
                startup_timeout_ms = parse_u64_arg("--startup-timeout-ms", args.next())?;
            }
            "--rollback-timeout-ms" => {
                rollback_timeout_ms = parse_u64_arg("--rollback-timeout-ms", args.next())?;
            }
            "--watch-window-seconds" => {
                watch_window_seconds = Some(parse_u64_arg("--watch-window-seconds", args.next())?);
            }
            "--sample-cadence-ms" => {
                sample_cadence_ms = parse_u64_arg("--sample-cadence-ms", args.next())?;
            }
            "--max-observation-staleness-ms" => {
                max_observation_staleness_ms = Some(parse_u64_arg(
                    "--max-observation-staleness-ms",
                    args.next(),
                )?);
            }
            "--json" => json = true,
            "--plan-cutover" => set_mode(&mut mode, Mode::PlanCutover, "--plan-cutover")?,
            "--render-cutover-script" => set_mode(
                &mut mode,
                Mode::RenderCutoverScript,
                "--render-cutover-script",
            )?,
            "--verify-cutover-target" => set_mode(
                &mut mode,
                Mode::VerifyCutoverTarget,
                "--verify-cutover-target",
            )?,
            "--plan-live-cutover" => {
                set_mode(&mut mode, Mode::PlanLiveCutover, "--plan-live-cutover")?
            }
            "--run-live-cutover" => {
                set_mode(&mut mode, Mode::RunLiveCutover, "--run-live-cutover")?
            }
            "--verify-cutover-session" => set_mode(
                &mut mode,
                Mode::VerifyCutoverSession,
                "--verify-cutover-session",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| {
        anyhow!(
            "select exactly one mode: --plan-cutover | --render-cutover-script | --verify-cutover-target | --plan-live-cutover | --run-live-cutover | --verify-cutover-session"
        )
    })?;
    let activation_config_path =
        activation_config_path.ok_or_else(|| anyhow!("missing required --activation-config"))?;
    let rollback_config_path =
        rollback_config_path.ok_or_else(|| anyhow!("missing required --rollback-config"))?;
    let runtime_dir = runtime_dir.ok_or_else(|| anyhow!("missing required --runtime-dir"))?;
    let target_config_path =
        target_config_path.ok_or_else(|| anyhow!("missing required --target-config"))?;
    let target_service_name =
        target_service_name.ok_or_else(|| anyhow!("missing required --target-service"))?;
    let service_control_command_path = service_control_command_path
        .ok_or_else(|| anyhow!("missing required --service-control-command"))?;
    let backup_dir = backup_dir.ok_or_else(|| anyhow!("missing required --backup-dir"))?;

    if sample_cadence_ms < MIN_SAMPLE_CADENCE_MS {
        bail!("--sample-cadence-ms must be >= {}", MIN_SAMPLE_CADENCE_MS);
    }

    match mode {
        Mode::RenderCutoverScript => {
            if output_path.is_none() {
                bail!("missing required --output for --render-cutover-script");
            }
            if session_dir.is_none() {
                bail!("missing required --session-dir for --render-cutover-script");
            }
        }
        Mode::RunLiveCutover | Mode::VerifyCutoverSession => {
            if session_dir.is_none() {
                bail!("missing required --session-dir");
            }
            if output_path.is_some() {
                bail!("--output is only valid with --render-cutover-script");
            }
        }
        _ => {
            if output_path.is_some() {
                bail!("--output is only valid with --render-cutover-script");
            }
        }
    }

    Ok(Some(Config {
        mode,
        activation_config_path,
        rollback_config_path,
        runtime_dir,
        target_config_path,
        target_service_name,
        service_control_command_path,
        backup_dir,
        session_dir,
        output_path,
        startup_timeout_ms: startup_timeout_ms.max(1_000),
        rollback_timeout_ms: rollback_timeout_ms.max(1_000),
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanCutover => plan_cutover_report(&config),
        Mode::RenderCutoverScript => render_cutover_script_report(&config),
        Mode::VerifyCutoverTarget => verify_cutover_target_report(&config),
        Mode::PlanLiveCutover => plan_live_cutover_report(&config),
        Mode::RunLiveCutover => run_live_cutover_report(&config),
        Mode::VerifyCutoverSession => verify_cutover_session_report(&config),
    }?;
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

fn plan_cutover_report(config: &Config) -> Result<CutoverReport> {
    let bundle = build_cutover_bundle(config)?;
    let (live_verdict, live_reason) = determine_live_cutover_readiness(&bundle);
    let (verdict, reason) = if live_verdict == TinyLiveCutoverVerdict::TinyLiveLiveCutoverPlanReady
    {
        (
            TinyLiveCutoverVerdict::TinyLiveCutoverPlanReady,
            "bounded live cutover contract is explicit over verify -> apply-live -> watch-live -> rollback-live, but it remains non-authorizing until a later explicit activation decision".to_string(),
        )
    } else {
        (
            TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid,
            live_reason,
        )
    };
    Ok(base_report(
        config,
        "plan_cutover",
        verdict,
        reason,
        &bundle,
    ))
}

fn render_cutover_script_report(config: &Config) -> Result<CutoverReport> {
    let plan = plan_cutover_report(config)?;
    if plan.verdict != TinyLiveCutoverVerdict::TinyLiveCutoverPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-cutover-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n{}\n",
        command_base_for_script(config)?
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(CutoverReport {
        generated_at: Utc::now(),
        mode: "render_cutover_script".to_string(),
        verdict: TinyLiveCutoverVerdict::TinyLiveCutoverScriptRendered,
        reason: "bounded live cutover wrapper script rendered".to_string(),
        explicit_statement:
            "rendered cutover script only wraps the bounded live cutover contract; it does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn verify_cutover_target_report(config: &Config) -> Result<CutoverReport> {
    let bundle = build_cutover_bundle(config)?;
    let mismatches = cutover_mismatches(&bundle);
    let verdict = if mismatches.is_empty() {
        TinyLiveCutoverVerdict::TinyLiveCutoverVerifyOk
    } else {
        TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        "bounded live cutover target, backup proof, current gate truth, and watch contract are coherent".to_string()
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "live cutover target verification failed".to_string())
    };
    let mut report = base_report(config, "verify_cutover_target", verdict, reason, &bundle);
    report.backup_proof_present = bundle
        .live_verify
        .live_verification
        .as_ref()
        .and_then(|value| value.backup_proof_present);
    report.verification_mismatches = mismatches;
    Ok(report)
}

fn plan_live_cutover_report(config: &Config) -> Result<CutoverReport> {
    let bundle = build_cutover_bundle(config)?;
    let (verdict, reason) = determine_live_cutover_readiness(&bundle);
    Ok(base_report(
        config,
        "plan_live_cutover",
        verdict,
        reason,
        &bundle,
    ))
}

fn run_live_cutover_report(config: &Config) -> Result<CutoverReport> {
    run_live_cutover_report_with_watch_runner(config, |config| {
        tiny_live_activation_watch::watch_live_target_report_for_drill(
            &config.activation_config_path,
            &config.rollback_config_path,
            &config.runtime_dir,
            &config.target_config_path,
            &config.target_service_name,
            &config.service_control_command_path,
            &config.backup_dir,
            config.watch_window_seconds,
            config.sample_cadence_ms,
            config.max_observation_staleness_ms,
        )
    })
}

fn run_live_cutover_report_with_watch_runner<F>(
    config: &Config,
    watch_runner: F,
) -> Result<CutoverReport>
where
    F: Fn(&Config) -> Result<tiny_live_activation_watch::WatchReport>,
{
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-cutover"))?;
    ensure_clean_cutover_session_dir(session_dir)?;
    ensure_clean_runtime_observation_for_cutover(&config.runtime_dir)?;

    let bundle = build_cutover_bundle(config)?;
    let paths = cutover_paths(session_dir);
    let session = planned_cutover_session(config, &bundle);
    persist_json(&paths.session_path, &session)?;

    let verify_report = verify_cutover_target_report(config)?;
    persist_json(&paths.verify_target_report_path, &verify_report)?;
    let verify_step = Some(step_from_cutover_report(
        &paths.verify_target_report_path,
        &verify_report,
    ));

    let (preflight_verdict, preflight_reason) = determine_live_cutover_readiness(&bundle);
    if preflight_verdict != TinyLiveCutoverVerdict::TinyLiveLiveCutoverPlanReady {
        let status = CutoverStatus {
            status_version: CUTOVER_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: LiveCutoverResult::FailedBeforeApply,
            reason: preflight_reason.clone(),
            decision: "failed_before_apply".to_string(),
            bounded_but_degraded: false,
            verify_target_step: verify_step.clone(),
            apply_step: None,
            watch_step: None,
            rollback_step: None,
            explicit_statement:
                "live cutover stopped before apply because current gate truth, backup proof, target contract, or watch readiness was non-green"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        let mut report = base_report(
            config,
            "run_live_cutover",
            preflight_verdict,
            preflight_reason,
            &bundle,
        );
        report.session_dir = Some(session_dir.display().to_string());
        report.cutover_session_path = Some(paths.session_path.display().to_string());
        report.cutover_status_path = Some(paths.status_path.display().to_string());
        report.live_result = Some(serialize_enum(&status.result));
        report.decision = Some(status.decision.clone());
        report.verify_target_step = verify_step;
        return Ok(report);
    }

    let apply_report = tiny_live_activation_live_execute::apply_live_report_for_cutover(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.target_config_path,
        &config.target_service_name,
        &config.service_control_command_path,
        &config.runtime_dir,
        &config.backup_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    persist_json(&paths.apply_report_path, &apply_report)?;
    let apply_step = Some(step_from_live_execute_report(
        &paths.apply_report_path,
        &apply_report,
    ));
    if apply_report.verdict
        != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted
    {
        let status = CutoverStatus {
            status_version: CUTOVER_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: LiveCutoverResult::FailedBeforeApply,
            reason: apply_report.reason.clone(),
            decision: "failed_before_apply".to_string(),
            bounded_but_degraded: false,
            verify_target_step: verify_step.clone(),
            apply_step: apply_step.clone(),
            watch_step: None,
            rollback_step: None,
            explicit_statement:
                "live cutover failed before watch because apply-live could not establish a trustworthy bounded activated target"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        let mut report = base_report(
            config,
            "run_live_cutover",
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverFailedBeforeApply,
            status.reason.clone(),
            &bundle,
        );
        report.session_dir = Some(session_dir.display().to_string());
        report.cutover_session_path = Some(paths.session_path.display().to_string());
        report.cutover_status_path = Some(paths.status_path.display().to_string());
        report.live_result = Some(serialize_enum(&status.result));
        report.decision = Some(status.decision.clone());
        report.verify_target_step = verify_step;
        report.apply_step = apply_step;
        return Ok(report);
    }

    let watch_report = watch_runner(config)?;
    persist_json(&paths.watch_report_path, &watch_report)?;
    let watch_step = Some(step_from_watch_report(
        &paths.watch_report_path,
        &watch_report,
    ));
    if watch_report.verdict
        == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchLiveContinue
    {
        let status = CutoverStatus {
            status_version: CUTOVER_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: LiveCutoverResult::CompletedKeepRunning,
            reason: watch_report.reason.clone(),
            decision: "keep_running".to_string(),
            bounded_but_degraded: watch_report.bounded_but_degraded,
            verify_target_step: verify_step.clone(),
            apply_step: apply_step.clone(),
            watch_step: watch_step.clone(),
            rollback_step: None,
            explicit_statement:
                "live cutover completed bounded apply -> watch and currently keeps the explicit live target running inside the accepted tiny-live envelope"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        let mut report = base_report(
            config,
            "run_live_cutover",
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverCompletedKeepRunning,
            status.reason.clone(),
            &bundle,
        );
        report.session_dir = Some(session_dir.display().to_string());
        report.cutover_session_path = Some(paths.session_path.display().to_string());
        report.cutover_status_path = Some(paths.status_path.display().to_string());
        report.live_result = Some(serialize_enum(&status.result));
        report.decision = Some(status.decision.clone());
        report.bounded_but_degraded = status.bounded_but_degraded;
        report.verify_target_step = verify_step;
        report.apply_step = apply_step;
        report.watch_step = watch_step;
        return Ok(report);
    }

    let rollback_report = tiny_live_activation_live_execute::rollback_live_report_for_cutover(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.target_config_path,
        &config.target_service_name,
        &config.service_control_command_path,
        &config.runtime_dir,
        &config.backup_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    persist_json(&paths.rollback_report_path, &rollback_report)?;
    let rollback_step = Some(step_from_live_execute_report(
        &paths.rollback_report_path,
        &rollback_report,
    ));
    let rollback_completed = rollback_report.verdict
        == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackCompleted;
    let (status_result, verdict, decision) = if rollback_completed {
        (
            LiveCutoverResult::CompletedWithRollback,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverCompletedWithRollback,
            "rollback_completed",
        )
    } else {
        (
            LiveCutoverResult::FailedDuringWatch,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverFailedDuringWatch,
            "rollback_failed",
        )
    };
    let status = CutoverStatus {
        status_version: CUTOVER_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        result: status_result,
        reason: rollback_report.reason.clone(),
        decision: decision.to_string(),
        bounded_but_degraded: watch_report.bounded_but_degraded,
        verify_target_step: verify_step.clone(),
        apply_step: apply_step.clone(),
        watch_step: watch_step.clone(),
        rollback_step: rollback_step.clone(),
        explicit_statement:
            "live cutover escalated from bounded watch into the paired rollback contract and persisted explicit evidence for the final outcome"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let mut report = base_report(
        config,
        "run_live_cutover",
        verdict,
        status.reason.clone(),
        &bundle,
    );
    report.session_dir = Some(session_dir.display().to_string());
    report.cutover_session_path = Some(paths.session_path.display().to_string());
    report.cutover_status_path = Some(paths.status_path.display().to_string());
    report.live_result = Some(serialize_enum(&status.result));
    report.decision = Some(status.decision.clone());
    report.bounded_but_degraded = status.bounded_but_degraded;
    report.verify_target_step = verify_step;
    report.apply_step = apply_step;
    report.watch_step = watch_step;
    report.rollback_step = rollback_step;
    Ok(report)
}

fn verify_cutover_session_report(config: &Config) -> Result<CutoverReport> {
    let bundle = build_cutover_bundle(config)?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-cutover-session"))?;
    let paths = cutover_paths(session_dir);
    let session: CutoverSession = load_json(&paths.session_path).with_context(|| {
        format!(
            "failed reading live cutover session {}",
            paths.session_path.display()
        )
    })?;
    let status: CutoverStatus = load_json(&paths.status_path).with_context(|| {
        format!(
            "failed reading live cutover status {}",
            paths.status_path.display()
        )
    })?;
    let mut mismatches = Vec::new();
    verify_cutover_session_matches_config(&session, config, &mut mismatches);

    let verify_target_report: Option<CutoverReport> = match &status.verify_target_step {
        Some(step) => match load_bound_step_json(
            step,
            &paths.verify_target_report_path,
            "verify_target_step",
            &mut mismatches,
        ) {
            Ok(value) => Some(value),
            Err(error) => {
                mismatches.push(error.to_string());
                None
            }
        },
        None => {
            mismatches.push("cutover session is missing verify-target evidence".to_string());
            None
        }
    };
    let apply_report: Option<tiny_live_activation_live_execute::LiveExecuteReport> =
        match &status.apply_step {
            Some(step) => match load_bound_step_json(
                step,
                &paths.apply_report_path,
                "apply_step",
                &mut mismatches,
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    mismatches.push(error.to_string());
                    None
                }
            },
            None => None,
        };
    let watch_report: Option<tiny_live_activation_watch::WatchReport> = match &status.watch_step {
        Some(step) => match load_bound_step_json(
            step,
            &paths.watch_report_path,
            "watch_step",
            &mut mismatches,
        ) {
            Ok(value) => Some(value),
            Err(error) => {
                mismatches.push(error.to_string());
                None
            }
        },
        None => None,
    };
    let rollback_report: Option<tiny_live_activation_live_execute::LiveExecuteReport> =
        match &status.rollback_step {
            Some(step) => match load_bound_step_json(
                step,
                &paths.rollback_report_path,
                "rollback_step",
                &mut mismatches,
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    mismatches.push(error.to_string());
                    None
                }
            },
            None => None,
        };

    if let Some(report) = &verify_target_report {
        if report.verdict != TinyLiveCutoverVerdict::TinyLiveCutoverVerifyOk {
            mismatches.push(
                "cutover session verify-target evidence must remain green for a coherent live cutover session"
                    .to_string(),
            );
        }
        if report.backup_proof_present != Some(true) {
            mismatches.push(
                "cutover session verify-target evidence must prove backup_proof_present=true"
                    .to_string(),
            );
        }
    }

    match status.result {
        LiveCutoverResult::CompletedKeepRunning => verify_keep_running_session(
            config,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        LiveCutoverResult::CompletedWithRollback => verify_rollback_session(
            config,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        LiveCutoverResult::FailedBeforeApply => verify_failed_before_apply_session(
            config,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        LiveCutoverResult::FailedDuringWatch => verify_failed_during_watch_session(
            config,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
    }

    let verdict = if mismatches.is_empty() {
        TinyLiveCutoverVerdict::TinyLiveCutoverVerifyOk
    } else {
        TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        "live cutover session artifacts, sequencing, and final posture remain coherent under the bounded live-target contract".to_string()
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "live cutover session verification failed".to_string())
    };
    let mut report = base_report(config, "verify_cutover_session", verdict, reason, &bundle);
    report.session_dir = Some(session_dir.display().to_string());
    report.cutover_session_path = Some(paths.session_path.display().to_string());
    report.cutover_status_path = Some(paths.status_path.display().to_string());
    report.live_result = Some(serialize_enum(&status.result));
    report.decision = Some(status.decision.clone());
    report.bounded_but_degraded = status.bounded_but_degraded;
    report.verify_target_step = status.verify_target_step.clone();
    report.apply_step = status.apply_step.clone();
    report.watch_step = status.watch_step.clone();
    report.rollback_step = status.rollback_step.clone();
    report.verification_mismatches = mismatches;
    Ok(report)
}

#[allow(dead_code)]
pub(crate) fn plan_live_cutover_for_package_inputs(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    session_dir: &Path,
) -> Result<PackageCutoverPlanSummary> {
    let config = Config {
        mode: Mode::PlanLiveCutover,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        session_dir: Some(session_dir.to_path_buf()),
        output_path: None,
        startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
        rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
        watch_window_seconds: None,
        sample_cadence_ms: DEFAULT_SAMPLE_CADENCE_MS,
        max_observation_staleness_ms: None,
        json: false,
    };
    let report = plan_live_cutover_report(&config)?;
    Ok(PackageCutoverPlanSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        cutover_plan_command_summary: Some(command_base_for_script(&config)?),
        backup_command_summary: report.backup_command_summary,
        verify_command_summary: report.verify_command_summary,
        watch_live_command_summary: report.watch_live_command_summary,
        rollback_command_summary: report.rollback_command_summary,
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason,
        verification_mismatches: report.verification_mismatches,
        activation_authorized: report.activation_authorized,
    })
}

#[allow(dead_code)]
pub(crate) fn run_live_cutover_for_rehearsal(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    session_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
    observation_seed: &CutoverRehearsalObservationSeed,
) -> Result<CutoverRehearsalStep> {
    let config = rehearsal_config(
        Mode::RunLiveCutover,
        activation_config_path,
        rollback_config_path,
        runtime_dir,
        target_config_path,
        target_service_name,
        service_control_command_path,
        backup_dir,
        session_dir,
        startup_timeout_ms,
        rollback_timeout_ms,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
    );
    let report = run_live_cutover_report_with_watch_runner(&config, |config| {
        write_rehearsal_observation(config, observation_seed)?;
        tiny_live_activation_watch::watch_live_target_report_for_drill(
            &config.activation_config_path,
            &config.rollback_config_path,
            &config.runtime_dir,
            &config.target_config_path,
            &config.target_service_name,
            &config.service_control_command_path,
            &config.backup_dir,
            config.watch_window_seconds,
            config.sample_cadence_ms,
            config.max_observation_staleness_ms,
        )
    })?;
    cutover_rehearsal_step_from_report(&report)
}

#[allow(dead_code)]
pub(crate) fn verify_live_cutover_session_for_rehearsal(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    session_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Result<CutoverRehearsalStep> {
    let config = rehearsal_config(
        Mode::VerifyCutoverSession,
        activation_config_path,
        rollback_config_path,
        runtime_dir,
        target_config_path,
        target_service_name,
        service_control_command_path,
        backup_dir,
        session_dir,
        startup_timeout_ms,
        rollback_timeout_ms,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
    );
    let report = verify_cutover_session_report(&config)?;
    cutover_rehearsal_step_from_report(&report)
}

fn rehearsal_config(
    mode: Mode,
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    session_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Config {
    Config {
        mode,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        session_dir: Some(session_dir.to_path_buf()),
        output_path: None,
        startup_timeout_ms,
        rollback_timeout_ms,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json: false,
    }
}

fn cutover_rehearsal_step_from_report(report: &CutoverReport) -> Result<CutoverRehearsalStep> {
    Ok(CutoverRehearsalStep {
        report_json: serde_json::to_value(report)?,
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        session_dir: report.session_dir.clone(),
        cutover_session_path: report.cutover_session_path.clone(),
        cutover_status_path: report.cutover_status_path.clone(),
        live_result: report.live_result.clone(),
        decision: report.decision.clone(),
        bounded_but_degraded: report.bounded_but_degraded,
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason.clone(),
        verification_mismatches: report.verification_mismatches.clone(),
        activation_authorized: report.activation_authorized,
    })
}

fn write_rehearsal_observation(
    config: &Config,
    observation_seed: &CutoverRehearsalObservationSeed,
) -> Result<()> {
    fs::create_dir_all(&config.runtime_dir).with_context(|| {
        format!(
            "failed creating cutover rehearsal runtime dir {}",
            config.runtime_dir.display()
        )
    })?;
    let watch_plan = tiny_live_activation_watch::build_plan_watch_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        Some(&config.target_config_path),
        Some(&config.target_service_name),
        Some(&config.service_control_command_path),
        Some(&config.backup_dir),
        config.watch_window_seconds,
        config.sample_cadence_ms,
        config.max_observation_staleness_ms,
    )?;
    let activation_artifact = tiny_live_activation_execute::inspect_rendered_config_artifact(
        &config.activation_config_path,
    )
    .with_context(|| {
        format!(
            "failed inspecting activation artifact {} for cutover rehearsal observation",
            config.activation_config_path.display()
        )
    })?;
    let observed_at = observation_seed.observed_at.unwrap_or_else(Utc::now);
    let sample_window_started_at = observation_seed
        .sample_window_started_at
        .unwrap_or_else(|| {
            observed_at - ChronoDuration::seconds(watch_plan.watch_window_seconds as i64 + 60)
        });
    let observation = serde_json::json!({
        "observation_version": "1",
        "watch_target_kind": "live_target",
        "observed_at": observed_at,
        "sample_window_started_at": sample_window_started_at,
        "runtime_dir": config.runtime_dir.display().to_string(),
        "activation_config_path": config.activation_config_path.display().to_string(),
        "rollback_config_path": config.rollback_config_path.display().to_string(),
        "target_config_path": config.target_config_path.display().to_string(),
        "target_service_name": config.target_service_name,
        "source_config_fingerprint_sha256": activation_artifact.metadata.source_config_fingerprint_sha256,
        "target_running": observation_seed.target_running,
        "total_execution_attempts": observation_seed.total_execution_attempts,
        "execution_error_count": observation_seed.execution_error_count,
        "adapter_contract_failure_count": observation_seed.adapter_contract_failure_count,
        "policy_echo_mismatch_count": observation_seed.policy_echo_mismatch_count,
        "fee_or_slippage_breach_count": observation_seed.fee_or_slippage_breach_count,
        "connectivity_degraded_seconds": observation_seed.connectivity_degraded_seconds,
        "daily_realized_loss_sol": observation_seed.daily_realized_loss_sol,
        "consecutive_hard_failures": observation_seed.consecutive_hard_failures,
        "note": observation_seed.note,
    });
    fs::write(
        tiny_live_activation_watch::observation_path_for_drill(&config.runtime_dir),
        serde_json::to_string_pretty(&observation)?,
    )
    .with_context(|| {
        format!(
            "failed writing cutover rehearsal observation {}",
            tiny_live_activation_watch::observation_path_for_drill(&config.runtime_dir).display()
        )
    })
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &CutoverStepArtifact,
    expected_path: &Path,
    step_label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "cutover session {step_label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
        bail!(
            "refusing to trust cutover session {step_label} evidence outside deterministic session path {}",
            expected_path.display()
        );
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic cutover session {step_label} report {}",
            expected_path.display()
        )
    })
}

fn build_cutover_bundle(config: &Config) -> Result<CutoverBundle> {
    let max_observation_staleness_ms = config.max_observation_staleness_ms.unwrap_or_else(|| {
        std::cmp::max(
            MIN_OBSERVATION_STALENESS_MS,
            config
                .sample_cadence_ms
                .saturating_mul(OBSERVATION_STALENESS_MULTIPLIER),
        )
    });
    let watch_plan = tiny_live_activation_watch::build_plan_watch_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        Some(&config.target_config_path),
        Some(&config.target_service_name),
        Some(&config.service_control_command_path),
        Some(&config.backup_dir),
        config.watch_window_seconds,
        config.sample_cadence_ms,
        Some(max_observation_staleness_ms),
    )?;
    let watch_window_seconds = watch_plan.watch_window_seconds;
    let live_plan = tiny_live_activation_live_execute::build_plan_live_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.target_config_path,
        &config.target_service_name,
        &config.service_control_command_path,
        &config.runtime_dir,
        &config.backup_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    let live_verify = tiny_live_activation_live_execute::verify_live_target_report_for_cutover(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.target_config_path,
        &config.target_service_name,
        &config.service_control_command_path,
        &config.runtime_dir,
        &config.backup_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    Ok(CutoverBundle {
        live_plan,
        live_verify,
        watch_plan,
        watch_window_seconds,
        max_observation_staleness_ms,
    })
}

fn determine_live_cutover_readiness(bundle: &CutoverBundle) -> (TinyLiveCutoverVerdict, String) {
    match bundle.live_plan.verdict {
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady => {}
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3 => {
            return (
                TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByStage3,
                bundle.live_plan.reason.clone(),
            )
        }
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate => {
            return (
                TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByPreActivationGate,
                bundle.live_plan.reason.clone(),
            )
        }
        _ => {
            return (
                TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByInvalidTarget,
                bundle.live_plan.reason.clone(),
            )
        }
    }
    if bundle.watch_plan.verdict
        != tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchPlanReady
    {
        return (
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByInvalidTarget,
            bundle.watch_plan.reason.clone(),
        );
    }
    if bundle
        .live_verify
        .live_verification
        .as_ref()
        .and_then(|value| value.backup_proof_present)
        == Some(false)
    {
        return (
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByMissingBackup,
            bundle.live_verify.reason.clone(),
        );
    }
    if bundle.live_verify.verdict
        == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyOk
    {
        return (
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverPlanReady,
            "bounded live cutover contract is explicit over verify-target -> apply-live -> watch-live -> rollback-live, but it still remains non-authorizing until a later explicit activation decision".to_string(),
        );
    }
    (
        TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByInvalidTarget,
        bundle.live_verify.reason.clone(),
    )
}

fn cutover_mismatches(bundle: &CutoverBundle) -> Vec<String> {
    let mut mismatches = Vec::new();
    if bundle.live_verify.verdict
        != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyOk
    {
        if let Some(verification) = &bundle.live_verify.live_verification {
            mismatches.extend(verification.mismatches.clone());
        } else {
            mismatches.push(bundle.live_verify.reason.clone());
        }
    }
    if bundle.watch_plan.verdict
        != tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchPlanReady
    {
        mismatches.push(bundle.watch_plan.reason.clone());
    }
    mismatches
}

fn ensure_clean_cutover_session_dir(session_dir: &Path) -> Result<()> {
    let paths = cutover_paths(session_dir);
    for path in [
        &paths.session_path,
        &paths.status_path,
        &paths.verify_target_report_path,
        &paths.apply_report_path,
        &paths.watch_report_path,
        &paths.rollback_report_path,
    ] {
        if path.exists() {
            bail!(
                "refusing to reuse cutover session dir {}; remove existing cutover artifact {} first",
                session_dir.display(),
                path.display()
            );
        }
    }
    Ok(())
}

fn ensure_clean_runtime_observation_for_cutover(runtime_dir: &Path) -> Result<()> {
    let observation_path = tiny_live_activation_watch::observation_path_for_drill(runtime_dir);
    if observation_path.exists() {
        bail!(
            "refusing to reuse runtime dir {}; remove existing watch observation artifact {} first",
            runtime_dir.display(),
            observation_path.display()
        );
    }
    Ok(())
}

fn cutover_paths(session_dir: &Path) -> CutoverPaths {
    CutoverPaths {
        session_path: session_dir.join("tiny_live_activation_cutover.session.json"),
        status_path: session_dir.join("tiny_live_activation_cutover.status.json"),
        verify_target_report_path: session_dir
            .join("tiny_live_activation_cutover.verify_target.report.json"),
        apply_report_path: session_dir.join("tiny_live_activation_cutover.apply.report.json"),
        watch_report_path: session_dir.join("tiny_live_activation_cutover.watch.report.json"),
        rollback_report_path: session_dir.join("tiny_live_activation_cutover.rollback.report.json"),
    }
}

fn planned_cutover_session(config: &Config, bundle: &CutoverBundle) -> CutoverSession {
    CutoverSession {
        session_version: CUTOVER_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        session_dir: config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        watch_window_seconds: bundle.watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms: bundle.max_observation_staleness_ms,
        backup_command_summary: bundle.live_verify.backup_command_summary.clone(),
        verify_command_summary: bundle.live_verify.verify_command_summary.clone(),
        apply_command_summary: bundle.live_verify.apply_command_summary.clone(),
        watch_live_command_summary: bundle.watch_plan.watch_live_command_summary.clone(),
        rollback_command_summary: bundle.live_verify.rollback_command_summary.clone(),
        explicit_statement:
            "live cutover session metadata records bounded cutover sequencing only; it does not authorize production activation by itself"
                .to_string(),
    }
}

fn base_report(
    config: &Config,
    mode: &str,
    verdict: TinyLiveCutoverVerdict,
    reason: String,
    bundle: &CutoverBundle,
) -> CutoverReport {
    CutoverReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        session_dir: config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        watch_window_seconds: bundle.watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms: bundle.max_observation_staleness_ms,
        cutover_session_path: None,
        cutover_status_path: None,
        live_result: None,
        decision: None,
        bounded_but_degraded: false,
        verify_target_step: None,
        apply_step: None,
        watch_step: None,
        rollback_step: None,
        backup_command_summary: bundle.live_verify.backup_command_summary.clone(),
        verify_command_summary: bundle.live_verify.verify_command_summary.clone(),
        apply_command_summary: bundle.live_verify.apply_command_summary.clone(),
        watch_live_command_summary: bundle.watch_plan.watch_live_command_summary.clone(),
        rollback_command_summary: bundle.live_verify.rollback_command_summary.clone(),
        pre_activation_gate_verdict_used: bundle.live_verify.pre_activation_gate_verdict_used.clone(),
        pre_activation_gate_reason_used: bundle.live_verify.pre_activation_gate_reason_used.clone(),
        current_pre_activation_gate_verdict: bundle
            .live_verify
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: bundle
            .live_verify
            .current_pre_activation_gate_reason
            .clone(),
        activation_plan_verdict_used: bundle.live_verify.activation_plan_verdict_used.clone(),
        activation_plan_reason_used: bundle.live_verify.activation_plan_reason_used.clone(),
        guardrail_verdict_used: bundle.watch_plan.guardrail_verdict_used.clone(),
        guardrail_reason_used: bundle.watch_plan.guardrail_reason_used.clone(),
        backup_proof_present: bundle
            .live_verify
            .live_verification
            .as_ref()
            .and_then(|value| value.backup_proof_present),
        service_control_wrapper_verification: bundle
            .live_verify
            .live_verification
            .as_ref()
            .and_then(|value| value.service_control_wrapper_verification.clone()),
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this cutover orchestrator stitches together accepted bounded live-target primitives only; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn verify_cutover_session_matches_config(
    session: &CutoverSession,
    config: &Config,
    mismatches: &mut Vec<String>,
) {
    if session.activation_config_path != config.activation_config_path.display().to_string() {
        mismatches.push(
            "cutover session activation_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.rollback_config_path != config.rollback_config_path.display().to_string() {
        mismatches.push(
            "cutover session rollback_config_path does not match the explicit artifact".to_string(),
        );
    }
    if session.runtime_dir != config.runtime_dir.display().to_string() {
        mismatches.push(
            "cutover session runtime_dir does not match the explicit runtime dir".to_string(),
        );
    }
    if session.target_config_path != config.target_config_path.display().to_string() {
        mismatches.push(
            "cutover session target_config_path does not match the explicit live target"
                .to_string(),
        );
    }
    if session.target_service_name != config.target_service_name {
        mismatches.push(
            "cutover session target_service_name does not match the explicit live target"
                .to_string(),
        );
    }
    if session.service_control_command_path
        != config.service_control_command_path.display().to_string()
    {
        mismatches.push(
            "cutover session service_control_command_path does not match the explicit live target"
                .to_string(),
        );
    }
    if session.backup_dir != config.backup_dir.display().to_string() {
        mismatches
            .push("cutover session backup_dir does not match the explicit backup dir".to_string());
    }
    if session.session_dir
        != config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    {
        mismatches.push(
            "cutover session session_dir does not match the explicit session dir".to_string(),
        );
    }
}

fn verify_keep_running_session(
    config: &Config,
    status: &CutoverStatus,
    apply_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "keep_running" {
        mismatches
            .push("completed_keep_running status must keep decision=keep_running".to_string());
    }
    match apply_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted => {}
        Some(report) => mismatches.push(format!(
            "completed_keep_running cutover must persist apply_started, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("completed_keep_running cutover is missing apply evidence".to_string()),
    }
    match watch_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchLiveContinue => {}
        Some(report) => mismatches.push(format!(
            "completed_keep_running cutover must persist watch_live_continue, found {}",
            serialize_enum(&report.verdict)
        )),
        None => {
            mismatches.push("completed_keep_running cutover is missing watch evidence".to_string())
        }
    }
    if rollback_report.is_some() {
        mismatches
            .push("completed_keep_running cutover must not persist rollback evidence".to_string());
    }
    verify_target_matches_rendered_artifact(
        &config.target_config_path,
        &config.activation_config_path,
        true,
        mismatches,
    )?;
    Ok(())
}

fn verify_rollback_session(
    config: &Config,
    status: &CutoverStatus,
    apply_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "rollback_completed" {
        mismatches.push(
            "completed_with_rollback status must keep decision=rollback_completed".to_string(),
        );
    }
    match apply_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted => {}
        Some(report) => mismatches.push(format!(
            "completed_with_rollback cutover must persist apply_started, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("completed_with_rollback cutover is missing apply evidence".to_string()),
    }
    match watch_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered => {}
        Some(report) => mismatches.push(format!(
            "completed_with_rollback cutover must persist watch_live_rollback_triggered, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("completed_with_rollback cutover is missing watch evidence".to_string()),
    }
    match rollback_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackCompleted => {}
        Some(report) => mismatches.push(format!(
            "completed_with_rollback cutover must persist rollback_live_completed, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("completed_with_rollback cutover is missing rollback evidence".to_string()),
    }
    verify_target_matches_rendered_artifact(
        &config.target_config_path,
        &config.rollback_config_path,
        false,
        mismatches,
    )?;
    Ok(())
}

fn verify_failed_before_apply_session(
    config: &Config,
    status: &CutoverStatus,
    apply_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "failed_before_apply" {
        mismatches
            .push("failed_before_apply status must keep decision=failed_before_apply".to_string());
    }
    if watch_report.is_some() {
        mismatches.push("failed_before_apply cutover must not persist watch evidence".to_string());
    }
    if rollback_report.is_some() {
        mismatches
            .push("failed_before_apply cutover must not persist rollback evidence".to_string());
    }
    if let Some(report) = apply_report {
        if report.verdict
            == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted
        {
            mismatches.push(
                "failed_before_apply cutover cannot persist apply_started as the final apply verdict"
                    .to_string(),
            );
        }
    }
    let current_target = load_from_path(&config.target_config_path)?;
    if current_target.execution.enabled {
        mismatches.push(
            "failed_before_apply cutover must leave execution.enabled=false on the explicit target"
                .to_string(),
        );
    }
    Ok(())
}

fn verify_failed_during_watch_session(
    config: &Config,
    status: &CutoverStatus,
    apply_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_live_execute::LiveExecuteReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "rollback_failed" {
        mismatches
            .push("failed_during_watch status must keep decision=rollback_failed".to_string());
    }
    match apply_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted => {}
        Some(report) => mismatches.push(format!(
            "failed_during_watch cutover must persist apply_started, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("failed_during_watch cutover is missing apply evidence".to_string()),
    }
    match watch_report {
        Some(report)
            if report.verdict
                == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered => {}
        Some(report) => mismatches.push(format!(
            "failed_during_watch cutover must persist watch_live_rollback_triggered, found {}",
            serialize_enum(&report.verdict)
        )),
        None => mismatches.push("failed_during_watch cutover is missing watch evidence".to_string()),
    }
    if rollback_report.is_none() {
        mismatches.push("failed_during_watch cutover is missing rollback evidence".to_string());
    }
    let current_target = load_from_path(&config.target_config_path)?;
    if current_target.execution.enabled {
        mismatches.push(
            "failed_during_watch cutover must still leave an explainable disabled posture when rollback was attempted"
                .to_string(),
        );
    }
    Ok(())
}

fn verify_target_matches_rendered_artifact(
    target_config_path: &Path,
    rendered_config_path: &Path,
    expected_execution_enabled: bool,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    let current_target = load_from_path(target_config_path)?;
    let rendered =
        tiny_live_activation_execute::inspect_rendered_config_artifact(rendered_config_path)?;
    let current_fingerprint =
        tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &current_target,
        )?;
    let expected_fingerprint =
        tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &rendered.rendered_config,
        )?;
    if current_fingerprint.sha256 != expected_fingerprint.sha256 {
        mismatches.push(format!(
            "current target fingerprint {} does not match the rendered cutover artifact {}",
            current_fingerprint.sha256, expected_fingerprint.sha256
        ));
    }
    if current_target.execution.enabled != expected_execution_enabled {
        mismatches.push(format!(
            "current target execution.enabled is {} but cutover session expected {}",
            current_target.execution.enabled, expected_execution_enabled
        ));
    }
    Ok(())
}

fn step_from_cutover_report(path: &Path, report: &CutoverReport) -> CutoverStepArtifact {
    CutoverStepArtifact {
        path: path.display().to_string(),
        mode: report.mode.clone(),
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
    }
}

fn step_from_live_execute_report(
    path: &Path,
    report: &tiny_live_activation_live_execute::LiveExecuteReport,
) -> CutoverStepArtifact {
    CutoverStepArtifact {
        path: path.display().to_string(),
        mode: report.mode.clone(),
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
    }
}

fn step_from_watch_report(
    path: &Path,
    report: &tiny_live_activation_watch::WatchReport,
) -> CutoverStepArtifact {
    CutoverStepArtifact {
        path: path.display().to_string(),
        mode: report.mode.clone(),
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
    }
}

fn command_base_for_script(config: &Config) -> Result<String> {
    let exe =
        env::current_exe().with_context(|| "failed resolving current executable for cutover")?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for cutover script render"))?;
    let mut parts = vec![
        exe.display().to_string(),
        "--activation-config".to_string(),
        config.activation_config_path.display().to_string(),
        "--rollback-config".to_string(),
        config.rollback_config_path.display().to_string(),
        "--runtime-dir".to_string(),
        config.runtime_dir.display().to_string(),
        "--target-config".to_string(),
        config.target_config_path.display().to_string(),
        "--target-service".to_string(),
        config.target_service_name.clone(),
        "--service-control-command".to_string(),
        config.service_control_command_path.display().to_string(),
        "--backup-dir".to_string(),
        config.backup_dir.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--startup-timeout-ms".to_string(),
        config.startup_timeout_ms.to_string(),
        "--rollback-timeout-ms".to_string(),
        config.rollback_timeout_ms.to_string(),
        "--sample-cadence-ms".to_string(),
        config.sample_cadence_ms.to_string(),
        "--run-live-cutover".to_string(),
    ];
    if let Some(value) = config.watch_window_seconds {
        parts.push("--watch-window-seconds".to_string());
        parts.push(value.to_string());
    }
    if let Some(value) = config.max_observation_staleness_ms {
        parts.push("--max-observation-staleness-ms".to_string());
        parts.push(value.to_string());
    }
    Ok(parts.join(" "))
}

fn render_human(report: &CutoverReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("target_config={}", report.target_config_path),
        format!("target_service={}", report.target_service_name),
        format!("runtime_dir={}", report.runtime_dir),
        format!("backup_dir={}", report.backup_dir),
    ];
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.live_result {
        lines.push(format!("live_result={value}"));
    }
    if let Some(value) = &report.decision {
        lines.push(format!("decision={value}"));
    }
    lines.push(format!(
        "bounded_but_degraded={}",
        report.bounded_but_degraded
    ));
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = report.backup_proof_present {
        lines.push(format!("backup_proof_present={value}"));
    }
    if let Some(wrapper) = &report.service_control_wrapper_verification {
        lines.push(format!(
            "service_control_wrapper_verified={}",
            wrapper.mismatches.is_empty()
        ));
        lines.push(format!(
            "service_control_wrapper_exact_content_matches_expected={}",
            wrapper.exact_content_matches_expected
        ));
        if let Some(value) = &wrapper.wrapper_version {
            lines.push(format!("service_control_wrapper_version={value}"));
        }
        if let Some(value) = &wrapper.status_schema_version {
            lines.push(format!(
                "service_control_wrapper_status_schema_version={value}"
            ));
        }
        if let Some(value) = wrapper.timeout_ms {
            lines.push(format!("service_control_wrapper_timeout_ms={value}"));
        }
        if let Some(value) = &wrapper.backend_command {
            lines.push(format!("service_control_wrapper_backend_command={value}"));
        }
    }
    if let Some(value) = &report.cutover_session_path {
        lines.push(format!("cutover_session_path={value}"));
    }
    if let Some(value) = &report.cutover_status_path {
        lines.push(format!("cutover_status_path={value}"));
    }
    if let Some(value) = &report.backup_command_summary {
        lines.push(format!("backup_command={value}"));
    }
    if let Some(value) = &report.apply_command_summary {
        lines.push(format!("apply_command={value}"));
    }
    if let Some(value) = &report.watch_live_command_summary {
        lines.push(format!("watch_live_command={value}"));
    }
    if let Some(value) = &report.rollback_command_summary {
        lines.push(format!("rollback_command={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        for mismatch in &report.verification_mismatches {
            lines.push(format!("  - {mismatch}"));
        }
    }
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("refusing to overwrite existing output {}", path.display());
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

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_text_file(path, &serde_json::to_string_pretty(value)?)
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading json artifact {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing json artifact {}", path.display()))
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid u64 for {flag}: {raw}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("mode already selected before {flag}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn valid_target_matching_backup_and_green_watch_yield_keep_running_cutover() {
        let (fixture, report) =
            completed_keep_running_fixture_for_verify_test("tiny_live_cutover_keep_running");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverCompletedKeepRunning
        );
        assert_eq!(
            report.live_result.as_deref(),
            Some("completed_keep_running")
        );
        let verify = verify_cutover_session_report(&fixture.config).expect("verify session");
        assert_eq!(
            verify.verdict,
            TinyLiveCutoverVerdict::TinyLiveCutoverVerifyOk,
            "{verify:#?}"
        );
    }

    #[test]
    fn watch_triggered_breach_yields_bounded_rollback_classification() {
        let fixture = live_fixture("tiny_live_cutover_rollback", GateState::Green, true);
        let report = run_live_cutover_report_for_tests(
            &fixture.config,
            &fixture.source_fingerprint_sha256,
            12,
            3,
            None,
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverCompletedWithRollback
        );
        assert_eq!(
            report.live_result.as_deref(),
            Some("completed_with_rollback")
        );
        let target = load_from_path(&fixture.target_config_path).unwrap();
        assert!(!target.execution.enabled);
    }

    #[cfg(unix)]
    #[test]
    fn failure_before_apply_is_explicit() {
        let fixture = live_fixture("tiny_live_cutover_apply_fail", GateState::Green, true);
        fs::create_dir_all(&fixture.runtime_dir).unwrap();
        fs::write(fixture.runtime_dir.join("force_apply_exit_failure"), "1").unwrap();
        let report = run_live_cutover_report_for_tests(
            &fixture.config,
            &fixture.source_fingerprint_sha256,
            12,
            0,
            None,
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverFailedBeforeApply
        );
        assert_eq!(report.live_result.as_deref(), Some("failed_before_apply"));
    }

    #[cfg(unix)]
    #[test]
    fn failure_during_watch_is_explicit() {
        let report = failed_during_watch_fixture_for_test("tiny_live_cutover_watch_fail");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverFailedDuringWatch
        );
        assert_eq!(report.live_result.as_deref(), Some("failed_during_watch"));
    }

    #[test]
    fn missing_backup_proof_is_refused() {
        let fixture = live_fixture("tiny_live_cutover_missing_backup", GateState::Green, false);
        let report = plan_live_cutover_report(&fixture.config).expect("plan live cutover");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByMissingBackup
        );
    }

    #[test]
    fn stale_non_green_current_gate_blocks_live_cutover() {
        let fixture = live_fixture(
            "tiny_live_cutover_stage3_blocked",
            GateState::Stage3Blocked,
            true,
        );
        let report = plan_live_cutover_report(&fixture.config).expect("plan live cutover");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByStage3
        );
    }

    #[cfg(unix)]
    #[test]
    fn plan_live_cutover_refuses_non_wrapper_service_control_command() {
        let fixture = live_fixture("tiny_live_cutover_non_wrapper", GateState::Green, true);
        write_non_wrapper_service_control_command(&fixture.config.service_control_command_path);
        let report = plan_live_cutover_report(&fixture.config).expect("plan live cutover");
        assert_eq!(
            report.verdict,
            TinyLiveCutoverVerdict::TinyLiveLiveCutoverRefusedByInvalidTarget
        );
        assert!(report.reason.contains("repo-managed wrapper contract"));
    }

    #[test]
    fn verify_cutover_session_catches_missing_watch_artifact() {
        let (fixture, _report) = completed_keep_running_fixture_for_verify_test(
            "tiny_live_cutover_verify_missing_watch",
        );
        let paths = cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        fs::remove_file(&paths.watch_report_path).unwrap();
        let verify = verify_cutover_session_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid
        );
    }

    #[test]
    fn verify_cutover_session_rejects_tampered_watch_step_path() {
        let (fixture, _report) = completed_keep_running_fixture_for_verify_test(
            "tiny_live_cutover_verify_tampered_watch_step",
        );
        let paths = cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        let foreign_watch_path = fixture.runtime_dir.join("foreign.watch.report.json");
        fs::copy(&paths.watch_report_path, &foreign_watch_path).unwrap();
        let mut status: CutoverStatus = load_json(&paths.status_path).unwrap();
        status.watch_step.as_mut().unwrap().path = foreign_watch_path.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify = verify_cutover_session_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("watch_step path")
                && entry.contains("does not match deterministic session artifact")
        }));
    }

    #[test]
    fn verify_cutover_session_rejects_tampered_verify_target_step_path() {
        let (fixture, _report) = completed_keep_running_fixture_for_verify_test(
            "tiny_live_cutover_verify_tampered_verify_step",
        );
        let paths = cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        let foreign_verify_path = fixture
            .runtime_dir
            .join("foreign.verify_target.report.json");
        fs::copy(&paths.verify_target_report_path, &foreign_verify_path).unwrap();
        let mut status: CutoverStatus = load_json(&paths.status_path).unwrap();
        status.verify_target_step.as_mut().unwrap().path =
            foreign_verify_path.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify = verify_cutover_session_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveCutoverVerdict::TinyLiveCutoverVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("verify_target_step path")
                && entry.contains("does not match deterministic session artifact")
        }));
    }

    fn completed_keep_running_fixture_for_verify_test(label: &str) -> (LiveFixture, CutoverReport) {
        let mut last_report = None;
        for attempt in 0..3 {
            let fixture = live_fixture(&format!("{label}_{attempt}"), GateState::Green, true);
            let report = run_live_cutover_report_for_tests(
                &fixture.config,
                &fixture.source_fingerprint_sha256,
                12,
                0,
                None,
            )
            .expect("run live cutover");
            if report.verdict == TinyLiveCutoverVerdict::TinyLiveLiveCutoverCompletedKeepRunning {
                return (fixture, report);
            }
            last_report = Some(report);
        }
        let report = last_report.expect("last cutover report");
        panic!(
            "expected completed_keep_running cutover fixture but got {:?}: {}",
            report.verdict, report.reason
        );
    }

    #[cfg(unix)]
    fn failed_during_watch_fixture_for_test(label: &str) -> CutoverReport {
        let mut last_report = None;
        for attempt in 0..3 {
            let fixture = live_fixture(&format!("{label}_{attempt}"), GateState::Green, true);
            fs::create_dir_all(&fixture.runtime_dir).unwrap();
            fs::write(
                fixture.runtime_dir.join("force_rollback_status_mismatch"),
                "1",
            )
            .unwrap();
            let report = run_live_cutover_report_for_tests(
                &fixture.config,
                &fixture.source_fingerprint_sha256,
                12,
                3,
                None,
            )
            .expect("run live cutover");
            if report.verdict == TinyLiveCutoverVerdict::TinyLiveLiveCutoverFailedDuringWatch {
                return report;
            }
            last_report = Some(report);
        }
        let report = last_report.expect("last cutover report");
        panic!(
            "expected failed_during_watch cutover fixture but got {:?}: {}",
            report.verdict, report.reason
        );
    }

    fn run_live_cutover_report_for_tests(
        config: &Config,
        source_fingerprint_sha256: &str,
        total_execution_attempts: u64,
        execution_error_count: u64,
        observed_at: Option<DateTime<Utc>>,
    ) -> Result<CutoverReport> {
        run_live_cutover_report_with_watch_runner(config, |config| {
            write_watch_observation(
                &config.runtime_dir,
                &config.activation_config_path,
                &config.rollback_config_path,
                &config.target_config_path,
                &config.target_service_name,
                source_fingerprint_sha256,
                Utc::now()
                    - ChronoDuration::seconds(
                        config.watch_window_seconds.unwrap_or(900) as i64 + 60,
                    ),
                observed_at.unwrap_or_else(Utc::now),
                total_execution_attempts,
                execution_error_count,
            );
            tiny_live_activation_watch::watch_live_target_report_for_drill(
                &config.activation_config_path,
                &config.rollback_config_path,
                &config.runtime_dir,
                &config.target_config_path,
                &config.target_service_name,
                &config.service_control_command_path,
                &config.backup_dir,
                config.watch_window_seconds,
                config.sample_cadence_ms,
                config.max_observation_staleness_ms,
            )
        })
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
    }

    struct LiveFixture {
        _fixture_dir: PathBuf,
        target_config_path: PathBuf,
        runtime_dir: PathBuf,
        source_fingerprint_sha256: String,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn live_fixture(label: &str, gate_state: GateState, seed_backup: bool) -> LiveFixture {
        let fixture_dir = temp_dir(label);
        let runtime_dir = fixture_dir.join("runtime");
        let backup_dir = fixture_dir.join("backups");
        let session_dir = fixture_dir.join("cutover-session");
        let db_path = fixture_dir.join("live.db");
        let target_config_path = fixture_dir.join("live.server.toml");
        let activation_config_path = fixture_dir.join("rendered.activation.toml");
        let rollback_config_path = fixture_dir.join("rendered.rollback.toml");
        let service_control_command_path = fixture_dir.join("fake-service-control.sh");
        let rpc_server = MockHttpServer::spawn(
            8,
            json!({"jsonrpc":"2.0","id":1,"result":123456u64}).to_string(),
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            8,
            json!({
                "status": "ok",
                "route": "jito",
                "contract_version": "v1",
                "detail": "simulated"
            })
            .to_string(),
            Some("authorization"),
        );
        fs::write(
            &target_config_path,
            dynamic_live_config_toml(
                &db_path,
                &rpc_server.url(""),
                &adapter_server.url("/submit"),
                false,
            ),
        )
        .unwrap();
        seed_current_gate_state(&db_path, gate_state);
        write_fake_service_control_command(&service_control_command_path);
        let source = load_from_path(&target_config_path).unwrap();
        let source_fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source,
        )
        .unwrap();
        write_rendered_artifact_pair(
            &target_config_path,
            &activation_config_path,
            &rollback_config_path,
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
            &source_fingerprint.sha256,
        );
        if seed_backup {
            let backup =
                tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
                    &activation_config_path,
                    &rollback_config_path,
                    &target_config_path,
                    "solana-copy-bot.service",
                    &service_control_command_path,
                    &runtime_dir,
                    &backup_dir,
                    DEFAULT_STARTUP_TIMEOUT_MS,
                    DEFAULT_ROLLBACK_TIMEOUT_MS,
                )
                .unwrap();
            assert_eq!(
                backup.verdict,
                tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
            );
        }
        LiveFixture {
            _fixture_dir: fixture_dir,
            target_config_path: target_config_path.clone(),
            runtime_dir: runtime_dir.clone(),
            source_fingerprint_sha256: source_fingerprint.sha256,
            config: Config {
                mode: Mode::RunLiveCutover,
                activation_config_path,
                rollback_config_path,
                runtime_dir,
                target_config_path,
                target_service_name: "solana-copy-bot.service".to_string(),
                service_control_command_path,
                backup_dir,
                session_dir: Some(session_dir),
                output_path: None,
                startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
                rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
                watch_window_seconds: Some(900),
                sample_cadence_ms: 100,
                max_observation_staleness_ms: Some(60_000),
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn write_watch_observation(
        runtime_dir: &Path,
        activation_config_path: &Path,
        rollback_config_path: &Path,
        target_config_path: &Path,
        target_service_name: &str,
        source_fingerprint_sha256: &str,
        sample_window_started_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        total_execution_attempts: u64,
        execution_error_count: u64,
    ) {
        fs::create_dir_all(runtime_dir).unwrap();
        let observation = json!({
            "observation_version": "1",
            "watch_target_kind": "live_target",
            "observed_at": observed_at,
            "sample_window_started_at": sample_window_started_at,
            "runtime_dir": runtime_dir.display().to_string(),
            "activation_config_path": activation_config_path.display().to_string(),
            "rollback_config_path": rollback_config_path.display().to_string(),
            "target_config_path": target_config_path.display().to_string(),
            "target_service_name": target_service_name,
            "source_config_fingerprint_sha256": source_fingerprint_sha256,
            "target_running": true,
            "total_execution_attempts": total_execution_attempts,
            "execution_error_count": execution_error_count,
            "adapter_contract_failure_count": 0,
            "policy_echo_mismatch_count": 0,
            "fee_or_slippage_breach_count": 0,
            "connectivity_degraded_seconds": 0,
            "daily_realized_loss_sol": 0.0,
            "consecutive_hard_failures": 0,
            "note": "test"
        });
        fs::write(
            tiny_live_activation_watch::observation_path_for_drill(runtime_dir),
            serde_json::to_string_pretty(&observation).unwrap(),
        )
        .unwrap();
    }

    fn write_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
        source_fingerprint_sha256: &str,
    ) {
        write_rendered_artifact(
            activation_path,
            tiny_live_activation_execute::RenderKind::Activation,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, true),
            target_config_path,
            source_fingerprint_sha256,
            false,
            true,
        );
        write_rendered_artifact(
            rollback_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, false),
            target_config_path,
            source_fingerprint_sha256,
            false,
            false,
        );
    }

    fn write_rendered_artifact(
        path: &Path,
        render_kind: tiny_live_activation_execute::RenderKind,
        contents: String,
        source_config_path: &Path,
        source_fingerprint_sha256: &str,
        source_execution_enabled: bool,
        target_execution_enabled: bool,
    ) {
        fs::write(path, &contents).unwrap();
        let hash = format!("{:x}", Sha256::digest(contents.as_bytes()));
        let metadata = tiny_live_activation_execute::RenderedConfigMetadata {
            metadata_version: "1".to_string(),
            render_kind,
            generated_at: ts("2026-03-27T12:00:00Z"),
            input_config_path: source_config_path.display().to_string(),
            output_config_path: path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_execute::FINGERPRINT_SCOPE
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
            field_expectations: vec![tiny_live_activation_execute::FieldExpectation {
                field: "execution.enabled".to_string(),
                source_value: json!(source_execution_enabled),
                target_value: json!(target_execution_enabled),
                reason: "test".to_string(),
                source: "test".to_string(),
            }],
            execution_untouched_by_batch: true,
            activation_authorized: false,
            not_authorized_summary: "test".to_string(),
        };
        fs::write(
            tiny_live_activation_execute::metadata_path_for_rendered_config(path),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
        tiny_live_activation_execute::inspect_rendered_config_artifact(path).unwrap();
    }

    fn dynamic_live_config_toml(
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
        execution_enabled: bool,
    ) -> String {
        let mut contents = sample_activation_config_toml()
            .replace("/tmp/copybot-live.db", &db_path.display().to_string())
            .replace("https://rpc.example", rpc_url)
            .replace("http://127.0.0.1:8080/submit", adapter_url);
        if !execution_enabled {
            contents = contents.replacen("enabled = true", "enabled = false", 1);
        }
        contents
    }

    fn seed_current_gate_state(db_path: &Path, gate_state: GateState) {
        let store = SqliteStore::open(db_path).unwrap();
        let now = Utc::now();
        match gate_state {
            GateState::Green => {
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(5),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(15),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(25),
                    "validated_current",
                    "fresh_current",
                    true,
                );
            }
            GateState::Stage3Blocked => {
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(5),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(15),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - ChronoDuration::minutes(25),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
            }
        }
        if matches!(gate_state, GateState::Green) {
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - ChronoDuration::minutes(3),
                    "rehearsal_green",
                ))
                .unwrap();
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - ChronoDuration::minutes(12),
                    "rehearsal_green_with_business_reject",
                ))
                .unwrap();
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
                audit_json: json!({
                    "now": captured_at,
                    "window_start": captured_at - chrono::Duration::days(5),
                    "verdict": audit_verdict,
                    "reason": "seed",
                    "follow_top_n": 2,
                    "publication_truth_available": true,
                    "publication_runtime_mode": "healthy",
                    "publication_recent_under_gate": true,
                    "latest_publication_ts": captured_at,
                    "publication_age_seconds": 60,
                    "latest_publication_window_start": captured_at - chrono::Duration::days(5),
                    "published_scoring_source": "raw_window_persisted_stream",
                    "published_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "active_follow_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "current_raw_top_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "published_vs_current_raw": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": if exact_match { 2 } else { 1 },
                        "exact_match": exact_match,
                        "only_left": if exact_match { json!([]) } else { json!(["wallet-beta"]) },
                        "only_right": if exact_match { json!([]) } else { json!(["wallet-gamma"]) }
                    },
                    "active_follow_vs_current_raw": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": if exact_match { 2 } else { 1 },
                        "exact_match": exact_match,
                        "only_left": if exact_match { json!([]) } else { json!(["wallet-beta"]) },
                        "only_right": if exact_match { json!([]) } else { json!(["wallet-gamma"]) }
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
                        "covered_since": captured_at - chrono::Duration::days(5),
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
                            json!(["wallet-gamma"])
                        } else {
                            json!([])
                        },
                        "left_since_previous_cycle": [],
                        "stable_wallets_across_cycles": ["wallet-alpha", "wallet-beta"],
                        "unique_wallet_count_across_cycles": if exact_match { 3 } else { 2 },
                        "samples": []
                    }
                })
                .to_string(),
                shadow_signal_json: json!({
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

    fn rehearsal_write(
        rehearsed_at: DateTime<Utc>,
        verdict: &str,
    ) -> ExecutionDryRunRehearsalWrite {
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
            adapter_result_classification: verdict.to_string(),
            adapter_accepted: Some(true),
            adapter_detail: verdict.to_string(),
            policy_echo_present: true,
            route_echo_present: true,
            contract_version_echo_present: true,
            response_slippage_bps: None,
            response_tip_lamports: None,
            response_compute_unit_limit: None,
            response_compute_unit_price_micro_lamports: None,
            verdict: verdict.to_string(),
            reason: verdict.to_string(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            rehearsal_json: "{}".to_string(),
        }
    }

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
        let header_text = String::from_utf8_lossy(&header_bytes[..header_end]).to_string();
        let headers = header_text.lines().map(|line| line.to_string()).collect();
        Some(ParsedHttpRequest { headers })
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
        stream.flush().expect("flush response");
    }

    #[cfg(unix)]
    fn write_fake_service_control_command(path: &Path) {
        let backend_path = path.with_extension("backend.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
cmd="${1:-}"
service="${2:-}"
runtime_dir="${COPYBOT_LIVE_SERVICE_CONTROL_RUNTIME_DIR:-}"
action="${COPYBOT_LIVE_SERVICE_CONTROL_ACTION:-}"
mkdir -p "$runtime_dir"
case "$cmd" in
  restart)
    if [[ "$action" == "activation" && -f "$runtime_dir/force_apply_exit_failure" ]]; then
      echo "forced activation failure" >&2
      exit 7
    fi
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
        fs::write(&backend_path, script).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&backend_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&backend_path, perms).unwrap();
        live_service_control_wrapper_contract::render_wrapper_script(
            path,
            &backend_path.display().to_string(),
            live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
    }

    #[cfg(unix)]
    fn write_non_wrapper_service_control_command(path: &Path) {
        fs::write(path, "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    #[cfg(not(unix))]
    fn write_fake_service_control_command(path: &Path) {
        fs::write(path, "").unwrap();
    }

    fn sample_activation_config_toml() -> String {
        r#"
[system]
env = "prod-live"

[sqlite]
path = "/tmp/copybot-live.db"

[execution]
enabled = true
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
"#
        .trim_start()
        .to_string()
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&Utc)
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
}
