use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_apply.rs"]
pub(crate) mod tiny_live_activation_apply;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_execute.rs"]
pub(crate) mod tiny_live_activation_execute;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
pub(crate) mod tiny_live_activation_live_execute;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_watch.rs"]
pub(crate) mod tiny_live_activation_watch;

const USAGE: &str = "usage: copybot_tiny_live_activation_drill --activation-config <path> --rollback-config <path> --runtime-dir <path> [--target-config <path>] [--target-service <name>] [--service-control-command <path>] [--backup-dir <path>] [--output <path>] [--json] [--startup-timeout-ms <ms>] [--rollback-timeout-ms <ms>] [--watch-window-seconds <seconds>] [--sample-cadence-ms <ms>] [--max-observation-staleness-ms <ms>] (--plan-drill | --render-drill-script | --run-temp-drill | --verify-temp-drill | --plan-live-drill)";
const DRILL_SESSION_VERSION: &str = "1";
const DRILL_STATUS_VERSION: &str = "1";
const DEFAULT_TEMP_STARTUP_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_TEMP_ROLLBACK_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_LIVE_STARTUP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_LIVE_ROLLBACK_TIMEOUT_MS: u64 = 10_000;
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
    target_config_path: Option<PathBuf>,
    target_service_name: Option<String>,
    service_control_command_path: Option<PathBuf>,
    backup_dir: Option<PathBuf>,
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
    PlanDrill,
    RenderDrillScript,
    RunTempDrill,
    VerifyTempDrill,
    PlanLiveDrill,
    InternalTempRunWorker,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveDrillVerdict {
    TinyLiveDrillPlanReady,
    TinyLiveDrillScriptRendered,
    TinyLiveTempDrillCompletedKeepRunning,
    TinyLiveTempDrillCompletedWithRollback,
    TinyLiveTempDrillFailedBeforeApply,
    TinyLiveTempDrillFailedDuringWatch,
    TinyLiveTempDrillVerifyOk,
    TinyLiveTempDrillVerifyInvalid,
    TinyLiveLiveDrillPlanReady,
    TinyLiveLiveDrillRefusedByStage3,
    TinyLiveLiveDrillRefusedByPreActivationGate,
    TinyLiveLiveDrillRefusedByInvalidContract,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TempDrillResult {
    CompletedKeepRunning,
    CompletedWithRollback,
    FailedBeforeApply,
    FailedDuringWatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrillStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TempDrillSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: u64,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: u64,
    activation_metadata_path: Option<String>,
    rollback_metadata_path: Option<String>,
    apply_command_summary: Option<String>,
    watch_verify_command_summary: Option<String>,
    watch_temp_command_summary: Option<String>,
    rollback_command_summary: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TempDrillStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    runtime_dir: String,
    result: TempDrillResult,
    reason: String,
    decision: String,
    bounded_but_degraded: bool,
    apply_step: Option<DrillStepArtifact>,
    watch_step: Option<DrillStepArtifact>,
    rollback_step: Option<DrillStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct DrillPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    apply_report_path: PathBuf,
    watch_report_path: PathBuf,
    rollback_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrillReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveDrillVerdict,
    reason: String,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    target_config_path: Option<String>,
    target_service_name: Option<String>,
    service_control_command_path: Option<String>,
    backup_dir: Option<String>,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    watch_window_seconds: u64,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: u64,
    drill_session_path: Option<String>,
    drill_status_path: Option<String>,
    temp_result: Option<String>,
    apply_step: Option<DrillStepArtifact>,
    watch_step: Option<DrillStepArtifact>,
    rollback_step: Option<DrillStepArtifact>,
    apply_command_summary: Option<String>,
    watch_verify_command_summary: Option<String>,
    watch_temp_command_summary: Option<String>,
    watch_live_command_summary: Option<String>,
    rollback_command_summary: Option<String>,
    backup_command_summary: Option<String>,
    live_apply_command_summary: Option<String>,
    live_verify_command_summary: Option<String>,
    live_rollback_command_summary: Option<String>,
    pre_activation_gate_verdict_used: Option<String>,
    pre_activation_gate_reason_used: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_plan_verdict_used: Option<String>,
    activation_plan_reason_used: Option<String>,
    guardrail_verdict_used: Option<String>,
    guardrail_reason_used: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct TempPlanBundle {
    apply_plan: tiny_live_activation_apply::ApplyReport,
    watch_plan: tiny_live_activation_watch::WatchReport,
    watch_window_seconds: u64,
    max_observation_staleness_ms: u64,
}

#[derive(Debug, Clone)]
struct LivePlanBundle {
    live_plan: tiny_live_activation_live_execute::LiveExecuteReport,
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
    let mut output_path: Option<PathBuf> = None;
    let mut startup_timeout_ms = DEFAULT_TEMP_STARTUP_TIMEOUT_MS;
    let mut rollback_timeout_ms = DEFAULT_TEMP_ROLLBACK_TIMEOUT_MS;
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
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--startup-timeout-ms" => {
                startup_timeout_ms = parse_u64_arg("--startup-timeout-ms", args.next())?
            }
            "--rollback-timeout-ms" => {
                rollback_timeout_ms = parse_u64_arg("--rollback-timeout-ms", args.next())?
            }
            "--watch-window-seconds" => {
                watch_window_seconds = Some(parse_u64_arg("--watch-window-seconds", args.next())?)
            }
            "--sample-cadence-ms" => {
                sample_cadence_ms = parse_u64_arg("--sample-cadence-ms", args.next())?
            }
            "--max-observation-staleness-ms" => {
                max_observation_staleness_ms = Some(parse_u64_arg(
                    "--max-observation-staleness-ms",
                    args.next(),
                )?)
            }
            "--json" => json = true,
            "--plan-drill" => set_mode(&mut mode, Mode::PlanDrill, "--plan-drill")?,
            "--render-drill-script" => {
                set_mode(&mut mode, Mode::RenderDrillScript, "--render-drill-script")?
            }
            "--run-temp-drill" => set_mode(&mut mode, Mode::RunTempDrill, "--run-temp-drill")?,
            "--verify-temp-drill" => {
                set_mode(&mut mode, Mode::VerifyTempDrill, "--verify-temp-drill")?
            }
            "--plan-live-drill" => set_mode(&mut mode, Mode::PlanLiveDrill, "--plan-live-drill")?,
            "--internal-temp-run-worker" => set_mode(
                &mut mode,
                Mode::InternalTempRunWorker,
                "--internal-temp-run-worker",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| {
        anyhow!(
            "select exactly one mode: --plan-drill | --render-drill-script | --run-temp-drill | --verify-temp-drill | --plan-live-drill"
        )
    })?;
    let activation_config_path =
        activation_config_path.ok_or_else(|| anyhow!("missing required --activation-config"))?;
    let rollback_config_path =
        rollback_config_path.ok_or_else(|| anyhow!("missing required --rollback-config"))?;
    let runtime_dir = runtime_dir.ok_or_else(|| anyhow!("missing required --runtime-dir"))?;

    match mode {
        Mode::RenderDrillScript => {
            if output_path.is_none() {
                bail!("--output is required for --render-drill-script");
            }
        }
        Mode::InternalTempRunWorker => {
            if output_path.is_some() {
                bail!("--output is not valid with --internal-temp-run-worker");
            }
        }
        _ => {
            if output_path.is_some() {
                bail!("--output is only valid with --render-drill-script");
            }
        }
    }

    if sample_cadence_ms < MIN_SAMPLE_CADENCE_MS {
        bail!(
            "--sample-cadence-ms must be at least {}",
            MIN_SAMPLE_CADENCE_MS
        );
    }

    match mode {
        Mode::PlanLiveDrill => {
            if target_config_path.is_none() {
                bail!("missing required --target-config for --plan-live-drill");
            }
            if target_service_name.is_none() {
                bail!("missing required --target-service for --plan-live-drill");
            }
            if service_control_command_path.is_none() {
                bail!("missing required --service-control-command for --plan-live-drill");
            }
            if backup_dir.is_none() {
                bail!("missing required --backup-dir for --plan-live-drill");
            }
            startup_timeout_ms = startup_timeout_ms.max(DEFAULT_LIVE_STARTUP_TIMEOUT_MS);
            rollback_timeout_ms = rollback_timeout_ms.max(DEFAULT_LIVE_ROLLBACK_TIMEOUT_MS);
        }
        _ => {
            startup_timeout_ms = startup_timeout_ms.max(1_000);
            rollback_timeout_ms = rollback_timeout_ms.max(1_000);
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
        output_path,
        startup_timeout_ms,
        rollback_timeout_ms,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanDrill => plan_temp_drill_report(&config),
        Mode::RenderDrillScript => render_drill_script_report(&config),
        Mode::RunTempDrill => run_temp_drill_report(&config),
        Mode::VerifyTempDrill => verify_temp_drill_report(&config),
        Mode::PlanLiveDrill => plan_live_drill_report(&config),
        Mode::InternalTempRunWorker => {
            let report = tiny_live_activation_apply::run_internal_temp_run_worker_for_drill(
                &config.activation_config_path,
                &config.rollback_config_path,
                &config.runtime_dir,
                config.startup_timeout_ms,
                config.rollback_timeout_ms,
            )?;
            return Ok(if config.json {
                serde_json::to_string_pretty(&report)?
            } else {
                report.reason
            });
        }
    }?;
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

fn plan_temp_drill_report(config: &Config) -> Result<DrillReport> {
    let plan = build_temp_plan_bundle(config)?;
    let (verdict, reason) = if temp_plan_ready(&plan) {
        (
            TinyLiveDrillVerdict::TinyLiveDrillPlanReady,
            "bounded temp tiny-live drill contract is explicit; apply -> watch -> rollback sequencing remains non-authorizing and isolated from the real prod service".to_string(),
        )
    } else {
        (
            TinyLiveDrillVerdict::TinyLiveTempDrillFailedBeforeApply,
            first_temp_plan_failure_reason(&plan),
        )
    };
    Ok(base_report(
        config,
        plan.watch_window_seconds,
        plan.max_observation_staleness_ms,
    )
    .with_temp_plan(verdict, reason, &plan, None, None, None, Vec::new()))
}

fn render_drill_script_report(config: &Config) -> Result<DrillReport> {
    let plan_report = plan_temp_drill_report(config)?;
    if plan_report.verdict != TinyLiveDrillVerdict::TinyLiveDrillPlanReady {
        return Ok(plan_report);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-drill-script"))?;
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
    Ok(DrillReport {
        generated_at: Utc::now(),
        mode: "render_drill_script".to_string(),
        verdict: TinyLiveDrillVerdict::TinyLiveDrillScriptRendered,
        reason: "bounded temp tiny-live drill wrapper script rendered".to_string(),
        explicit_statement:
            "rendered drill script only orchestrates the bounded temp drill contract; it does not authorize or perform production activation by itself"
                .to_string(),
        ..plan_report
    })
}

fn run_temp_drill_report(config: &Config) -> Result<DrillReport> {
    run_temp_drill_report_with_apply_runner(config, |config| {
        tiny_live_activation_apply::apply_temp_run_for_drill(
            &config.activation_config_path,
            &config.rollback_config_path,
            &config.runtime_dir,
            config.startup_timeout_ms,
            config.rollback_timeout_ms,
        )
    })
}

fn run_temp_drill_report_with_apply_runner<F>(
    config: &Config,
    apply_runner: F,
) -> Result<DrillReport>
where
    F: Fn(&Config) -> Result<tiny_live_activation_apply::ApplyReport>,
{
    tiny_live_activation_apply::validate_runtime_dir(&config.runtime_dir)?;
    ensure_clean_temp_runtime_for_drill(&config.runtime_dir)?;

    let plan = build_temp_plan_bundle(config)?;
    let paths = drill_paths(&config.runtime_dir);
    let session = planned_temp_session(config, &plan);
    persist_temp_session(&paths.session_path, &session)?;
    fs::create_dir_all(&config.runtime_dir).with_context(|| {
        format!(
            "failed creating temp drill runtime dir {}",
            config.runtime_dir.display()
        )
    })?;

    if !temp_plan_ready(&plan) {
        persist_json(&paths.apply_report_path, &plan.apply_plan)?;
        persist_json(&paths.watch_report_path, &plan.watch_plan)?;
        let status = TempDrillStatus {
            status_version: DRILL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: TempDrillResult::FailedBeforeApply,
            reason: first_temp_plan_failure_reason(&plan),
            decision: "failed_before_apply".to_string(),
            bounded_but_degraded: false,
            apply_step: Some(step_from_apply_report(&paths.apply_report_path, &plan.apply_plan)),
            watch_step: Some(step_from_watch_report(&paths.watch_report_path, &plan.watch_plan)),
            rollback_step: None,
            explicit_statement:
                "temp drill stopped before apply because the accepted activation/apply/watch contracts were not simultaneously ready"
                    .to_string(),
        };
        persist_temp_status(&paths.status_path, &status)?;
        return Ok(base_report(
            config,
            plan.watch_window_seconds,
            plan.max_observation_staleness_ms,
        )
        .with_temp_plan(
            TinyLiveDrillVerdict::TinyLiveTempDrillFailedBeforeApply,
            status.reason.clone(),
            &plan,
            status.apply_step.clone(),
            status.watch_step.clone(),
            None,
            Vec::new(),
        )
        .with_temp_runtime_artifacts(&paths, &status));
    }

    let apply_report = apply_runner(config)?;
    persist_json(&paths.apply_report_path, &apply_report)?;
    if apply_report.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
    {
        let status = TempDrillStatus {
            status_version: DRILL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: TempDrillResult::FailedBeforeApply,
            reason: apply_report.reason.clone(),
            decision: "failed_before_apply".to_string(),
            bounded_but_degraded: false,
            apply_step: Some(step_from_apply_report(&paths.apply_report_path, &apply_report)),
            watch_step: None,
            rollback_step: None,
            explicit_statement:
                "temp drill failed before a bounded activation worker was established".to_string(),
        };
        persist_temp_status(&paths.status_path, &status)?;
        return Ok(
            base_report(config, plan.watch_window_seconds, plan.max_observation_staleness_ms)
                .with_temp_plan(
                    TinyLiveDrillVerdict::TinyLiveTempDrillFailedBeforeApply,
                    status.reason.clone(),
                    &plan,
                    status.apply_step.clone(),
                    None,
                    None,
                    Vec::new(),
                )
                .with_temp_runtime_artifacts(&paths, &status),
        );
    }

    let watch_report = tiny_live_activation_watch::watch_temp_run_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        Some(plan.watch_window_seconds),
        config.sample_cadence_ms,
        Some(plan.max_observation_staleness_ms),
    )?;
    persist_json(&paths.watch_report_path, &watch_report)?;

    // Persist each step outcome so verify-temp-drill can classify keep-running
    // vs rollback-completed vs failed-before-apply without shell archaeology.
    if watch_report.verdict
        == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchTempContinue
    {
        let status = TempDrillStatus {
            status_version: DRILL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            runtime_dir: config.runtime_dir.display().to_string(),
            result: TempDrillResult::CompletedKeepRunning,
            reason: watch_report.reason.clone(),
            decision: "keep_running".to_string(),
            bounded_but_degraded: watch_report.bounded_but_degraded,
            apply_step: Some(step_from_apply_report(&paths.apply_report_path, &apply_report)),
            watch_step: Some(step_from_watch_report(&paths.watch_report_path, &watch_report)),
            rollback_step: None,
            explicit_statement:
                "temp drill completed the bounded apply -> watch path and currently keeps the isolated rehearsal runtime running"
                    .to_string(),
        };
        persist_temp_status(&paths.status_path, &status)?;
        return Ok(base_report(
            config,
            plan.watch_window_seconds,
            plan.max_observation_staleness_ms,
        )
        .with_temp_plan(
            TinyLiveDrillVerdict::TinyLiveTempDrillCompletedKeepRunning,
            status.reason.clone(),
            &plan,
            status.apply_step.clone(),
            status.watch_step.clone(),
            None,
            Vec::new(),
        )
        .with_temp_runtime_artifacts(&paths, &status));
    }

    let rollback_report = tiny_live_activation_apply::rollback_temp_run_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    persist_json(&paths.rollback_report_path, &rollback_report)?;

    let rollback_completed = rollback_report.verdict
        == tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunCompleted;
    let result = if rollback_completed {
        TempDrillResult::CompletedWithRollback
    } else {
        TempDrillResult::FailedDuringWatch
    };
    let verdict = if rollback_completed {
        TinyLiveDrillVerdict::TinyLiveTempDrillCompletedWithRollback
    } else {
        TinyLiveDrillVerdict::TinyLiveTempDrillFailedDuringWatch
    };
    let reason = if rollback_completed {
        rollback_report.reason.clone()
    } else {
        rollback_report.reason.clone()
    };
    let status = TempDrillStatus {
        status_version: DRILL_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        runtime_dir: config.runtime_dir.display().to_string(),
        result,
        reason: reason.clone(),
        decision: if rollback_completed {
            "rollback_completed".to_string()
        } else {
            "rollback_failed".to_string()
        },
        bounded_but_degraded: watch_report.bounded_but_degraded,
        apply_step: Some(step_from_apply_report(&paths.apply_report_path, &apply_report)),
        watch_step: Some(step_from_watch_report(&paths.watch_report_path, &watch_report)),
        rollback_step: Some(step_from_apply_report(&paths.rollback_report_path, &rollback_report)),
        explicit_statement:
            "temp drill escalated from watch into the paired rollback contract and persisted explicit evidence for the final outcome"
                .to_string(),
    };
    persist_temp_status(&paths.status_path, &status)?;
    Ok(base_report(
        config,
        plan.watch_window_seconds,
        plan.max_observation_staleness_ms,
    )
    .with_temp_plan(
        verdict,
        reason,
        &plan,
        status.apply_step.clone(),
        status.watch_step.clone(),
        status.rollback_step.clone(),
        Vec::new(),
    )
    .with_temp_runtime_artifacts(&paths, &status))
}

fn verify_temp_drill_report(config: &Config) -> Result<DrillReport> {
    tiny_live_activation_apply::validate_runtime_dir(&config.runtime_dir)?;
    let paths = drill_paths(&config.runtime_dir);
    let session: TempDrillSession = load_json(&paths.session_path).with_context(|| {
        format!(
            "failed reading temp drill session {}",
            paths.session_path.display()
        )
    })?;
    let status: TempDrillStatus = load_json(&paths.status_path).with_context(|| {
        format!(
            "failed reading temp drill status {}",
            paths.status_path.display()
        )
    })?;
    let mut mismatches = Vec::new();
    verify_session_matches_config(&session, config, &mut mismatches);

    let apply_step = status.apply_step.clone();
    let watch_step = status.watch_step.clone();
    let rollback_step = status.rollback_step.clone();

    let apply_report: Option<tiny_live_activation_apply::ApplyReport> = match &apply_step {
        Some(step) => match load_json(Path::new(&step.path))
            .with_context(|| format!("failed reading temp drill apply report {}", step.path))
        {
            Ok(report) => Some(report),
            Err(error) => {
                mismatches.push(error.to_string());
                None
            }
        },
        None => None,
    };
    let watch_report: Option<tiny_live_activation_watch::WatchReport> = match &watch_step {
        Some(step) => match load_json(Path::new(&step.path))
            .with_context(|| format!("failed reading temp drill watch report {}", step.path))
        {
            Ok(report) => Some(report),
            Err(error) => {
                mismatches.push(error.to_string());
                None
            }
        },
        None => None,
    };
    let rollback_report: Option<tiny_live_activation_apply::ApplyReport> = match &rollback_step {
        Some(step) => match load_json(Path::new(&step.path))
            .with_context(|| format!("failed reading temp drill rollback report {}", step.path))
        {
            Ok(report) => Some(report),
            Err(error) => {
                mismatches.push(error.to_string());
                None
            }
        },
        None => None,
    };

    match status.result {
        TempDrillResult::CompletedKeepRunning => verify_keep_running_drill(
            config,
            &session,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        TempDrillResult::CompletedWithRollback => verify_rollback_drill(
            config,
            &session,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        TempDrillResult::FailedBeforeApply => verify_failed_before_apply_drill(
            config,
            &session,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
        TempDrillResult::FailedDuringWatch => verify_failed_during_watch_drill(
            config,
            &session,
            &status,
            apply_report.as_ref(),
            watch_report.as_ref(),
            rollback_report.as_ref(),
            &mut mismatches,
        )?,
    }

    let verdict = if mismatches.is_empty() {
        TinyLiveDrillVerdict::TinyLiveTempDrillVerifyOk
    } else {
        TinyLiveDrillVerdict::TinyLiveTempDrillVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        "temp drill artifacts, sequencing, and final posture remain coherent under the bounded rehearsal contract".to_string()
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "temp drill verification failed".to_string())
    };
    Ok(DrillReport {
        generated_at: Utc::now(),
        mode: "verify_temp_drill".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        target_config_path: None,
        target_service_name: None,
        service_control_command_path: None,
        backup_dir: None,
        startup_timeout_ms: session.startup_timeout_ms,
        rollback_timeout_ms: session.rollback_timeout_ms,
        watch_window_seconds: session.watch_window_seconds,
        sample_cadence_ms: session.sample_cadence_ms,
        max_observation_staleness_ms: session.max_observation_staleness_ms,
        drill_session_path: Some(paths.session_path.display().to_string()),
        drill_status_path: Some(paths.status_path.display().to_string()),
        temp_result: Some(serialize_enum(&status.result)),
        apply_step,
        watch_step,
        rollback_step,
        apply_command_summary: session.apply_command_summary,
        watch_verify_command_summary: session.watch_verify_command_summary,
        watch_temp_command_summary: session.watch_temp_command_summary,
        watch_live_command_summary: None,
        rollback_command_summary: session.rollback_command_summary,
        backup_command_summary: None,
        live_apply_command_summary: None,
        live_verify_command_summary: None,
        live_rollback_command_summary: None,
        pre_activation_gate_verdict_used: None,
        pre_activation_gate_reason_used: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        activation_plan_verdict_used: None,
        activation_plan_reason_used: None,
        guardrail_verdict_used: None,
        guardrail_reason_used: None,
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement:
            "verify-temp-drill only validates bounded rehearsal sequencing and final evidence; it does not authorize production activation"
                .to_string(),
    })
}

fn plan_live_drill_report(config: &Config) -> Result<DrillReport> {
    let plan = build_live_plan_bundle(config)?;
    let (verdict, reason) = match plan.live_plan.verdict {
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
            if plan.watch_plan.verdict
                == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchPlanReady =>
        {
            (
                TinyLiveDrillVerdict::TinyLiveLiveDrillPlanReady,
                "bounded live-target drill contract is explicit over backup -> apply -> watch -> rollback, but it still remains non-authorizing until a later explicit activation decision".to_string(),
            )
        }
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3 => (
            TinyLiveDrillVerdict::TinyLiveLiveDrillRefusedByStage3,
            plan.live_plan.reason.clone(),
        ),
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate => (
            TinyLiveDrillVerdict::TinyLiveLiveDrillRefusedByPreActivationGate,
            plan.live_plan.reason.clone(),
        ),
        _ => (
            TinyLiveDrillVerdict::TinyLiveLiveDrillRefusedByInvalidContract,
            if plan.live_plan.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
            {
                plan.live_plan.reason.clone()
            } else {
                plan.watch_plan.reason.clone()
            },
        ),
    };
    Ok(DrillReport {
        generated_at: Utc::now(),
        mode: "plan_live_drill".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        target_config_path: config
            .target_config_path
            .as_ref()
            .map(|path| path.display().to_string()),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config
            .service_control_command_path
            .as_ref()
            .map(|path| path.display().to_string()),
        backup_dir: config.backup_dir.as_ref().map(|path| path.display().to_string()),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        watch_window_seconds: plan.watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms: plan.max_observation_staleness_ms,
        drill_session_path: None,
        drill_status_path: None,
        temp_result: None,
        apply_step: None,
        watch_step: None,
        rollback_step: None,
        apply_command_summary: None,
        watch_verify_command_summary: plan.watch_plan.verify_watch_command_summary.clone(),
        watch_temp_command_summary: None,
        watch_live_command_summary: plan.watch_plan.watch_live_command_summary.clone(),
        rollback_command_summary: None,
        backup_command_summary: plan.live_plan.backup_command_summary.clone(),
        live_apply_command_summary: plan.live_plan.apply_command_summary.clone(),
        live_verify_command_summary: plan.live_plan.verify_command_summary.clone(),
        live_rollback_command_summary: plan.live_plan.rollback_command_summary.clone(),
        pre_activation_gate_verdict_used: plan.live_plan.pre_activation_gate_verdict_used.clone(),
        pre_activation_gate_reason_used: plan.live_plan.pre_activation_gate_reason_used.clone(),
        current_pre_activation_gate_verdict: plan.live_plan.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: plan.live_plan.current_pre_activation_gate_reason.clone(),
        activation_plan_verdict_used: plan.live_plan.activation_plan_verdict_used.clone(),
        activation_plan_reason_used: plan.live_plan.activation_plan_reason_used.clone(),
        guardrail_verdict_used: plan.watch_plan.guardrail_verdict_used.clone(),
        guardrail_reason_used: plan.watch_plan.guardrail_reason_used.clone(),
        verification_mismatches: plan
            .live_plan
            .live_verification
            .as_ref()
            .map(|value| value.mismatches.clone())
            .unwrap_or_default(),
        activation_authorized: false,
        explicit_statement:
            "plan-live-drill only prepares the bounded live-target contract; it does not authorize or perform production activation by itself"
                .to_string(),
    })
}

fn build_temp_plan_bundle(config: &Config) -> Result<TempPlanBundle> {
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
        None,
        None,
        None,
        None,
        config.watch_window_seconds,
        config.sample_cadence_ms,
        Some(max_observation_staleness_ms),
    )?;
    let watch_window_seconds = watch_plan.watch_window_seconds;
    let apply_plan = tiny_live_activation_apply::plan_temp_apply_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        config.startup_timeout_ms,
        config.rollback_timeout_ms,
    )?;
    Ok(TempPlanBundle {
        apply_plan,
        watch_plan,
        watch_window_seconds,
        max_observation_staleness_ms,
    })
}

fn build_live_plan_bundle(config: &Config) -> Result<LivePlanBundle> {
    let target_config_path = config
        .target_config_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --target-config for --plan-live-drill"))?;
    let target_service_name = config
        .target_service_name
        .as_deref()
        .ok_or_else(|| anyhow!("missing --target-service for --plan-live-drill"))?;
    let service_control_command_path = config
        .service_control_command_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --service-control-command for --plan-live-drill"))?;
    let backup_dir = config
        .backup_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --backup-dir for --plan-live-drill"))?;
    let max_observation_staleness_ms = config.max_observation_staleness_ms.unwrap_or_else(|| {
        std::cmp::max(
            MIN_OBSERVATION_STALENESS_MS,
            config
                .sample_cadence_ms
                .saturating_mul(OBSERVATION_STALENESS_MULTIPLIER),
        )
    });
    let live_plan = tiny_live_activation_live_execute::build_plan_live_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        target_config_path,
        target_service_name,
        service_control_command_path,
        &config.runtime_dir,
        backup_dir,
        config
            .startup_timeout_ms
            .max(DEFAULT_LIVE_STARTUP_TIMEOUT_MS),
        config
            .rollback_timeout_ms
            .max(DEFAULT_LIVE_ROLLBACK_TIMEOUT_MS),
    )?;
    let watch_plan = tiny_live_activation_watch::build_plan_watch_report_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        Some(target_config_path),
        Some(target_service_name),
        Some(service_control_command_path),
        Some(backup_dir),
        config.watch_window_seconds,
        config.sample_cadence_ms,
        Some(max_observation_staleness_ms),
    )?;
    Ok(LivePlanBundle {
        live_plan,
        watch_window_seconds: watch_plan.watch_window_seconds,
        max_observation_staleness_ms,
        watch_plan,
    })
}

fn temp_plan_ready(plan: &TempPlanBundle) -> bool {
    plan.apply_plan.verdict
        == tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationApplyPlanReady
        && plan.watch_plan.verdict
            == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchPlanReady
}

fn first_temp_plan_failure_reason(plan: &TempPlanBundle) -> String {
    if plan.apply_plan.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationApplyPlanReady
    {
        plan.apply_plan.reason.clone()
    } else {
        plan.watch_plan.reason.clone()
    }
}

fn ensure_clean_temp_runtime_for_drill(runtime_dir: &Path) -> Result<()> {
    let drill_paths = drill_paths(runtime_dir);
    let apply_paths = tiny_live_activation_apply::runtime_paths(runtime_dir);
    let watch_observation_path =
        tiny_live_activation_watch::observation_path_for_drill(runtime_dir);
    for path in [
        &drill_paths.session_path,
        &drill_paths.status_path,
        &drill_paths.apply_report_path,
        &drill_paths.watch_report_path,
        &drill_paths.rollback_report_path,
        &apply_paths.session_path,
        &apply_paths.pid_path,
        &apply_paths.log_path,
        &apply_paths.status_path,
        &apply_paths.rollback_status_path,
        &apply_paths.stop_request_path,
        &watch_observation_path,
    ] {
        if path.exists() {
            bail!(
                "refusing to reuse runtime dir {}; remove existing drill/runtime artifact {} first",
                runtime_dir.display(),
                path.display()
            );
        }
    }
    Ok(())
}

fn drill_paths(runtime_dir: &Path) -> DrillPaths {
    DrillPaths {
        session_path: runtime_dir.join("tiny_live_activation_drill.session.json"),
        status_path: runtime_dir.join("tiny_live_activation_drill.status.json"),
        apply_report_path: runtime_dir.join("tiny_live_activation_drill.apply.report.json"),
        watch_report_path: runtime_dir.join("tiny_live_activation_drill.watch.report.json"),
        rollback_report_path: runtime_dir.join("tiny_live_activation_drill.rollback.report.json"),
    }
}

fn planned_temp_session(config: &Config, plan: &TempPlanBundle) -> TempDrillSession {
    TempDrillSession {
        session_version: DRILL_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        watch_window_seconds: plan.watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms: plan.max_observation_staleness_ms,
        activation_metadata_path: plan.apply_plan.activation_metadata_path.clone(),
        rollback_metadata_path: plan.apply_plan.rollback_metadata_path.clone(),
        apply_command_summary: plan.apply_plan.apply_command_summary.clone(),
        watch_verify_command_summary: plan.watch_plan.verify_watch_command_summary.clone(),
        watch_temp_command_summary: plan.watch_plan.watch_temp_command_summary.clone(),
        rollback_command_summary: plan.apply_plan.rollback_command_summary.clone(),
        explicit_statement:
            "temp drill session ties together the accepted apply and watch contracts only; it does not authorize production activation"
                .to_string(),
    }
}

fn persist_temp_session(path: &Path, session: &TempDrillSession) -> Result<()> {
    persist_json(path, session)
}

fn persist_temp_status(path: &Path, status: &TempDrillStatus) -> Result<()> {
    persist_json(path, status)
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_text_file(path, &serde_json::to_string_pretty(value)?)
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed parsing {}", path.display()))
}

fn step_from_apply_report(
    path: &Path,
    report: &tiny_live_activation_apply::ApplyReport,
) -> DrillStepArtifact {
    DrillStepArtifact {
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
) -> DrillStepArtifact {
    DrillStepArtifact {
        path: path.display().to_string(),
        mode: report.mode.clone(),
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
    }
}

fn base_report(
    config: &Config,
    watch_window_seconds: u64,
    max_observation_staleness_ms: u64,
) -> DrillReport {
    DrillReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict: TinyLiveDrillVerdict::TinyLiveDrillPlanReady,
        reason: String::new(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        target_config_path: config
            .target_config_path
            .as_ref()
            .map(|path| path.display().to_string()),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config
            .service_control_command_path
            .as_ref()
            .map(|path| path.display().to_string()),
        backup_dir: config.backup_dir.as_ref().map(|path| path.display().to_string()),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms,
        drill_session_path: None,
        drill_status_path: None,
        temp_result: None,
        apply_step: None,
        watch_step: None,
        rollback_step: None,
        apply_command_summary: None,
        watch_verify_command_summary: None,
        watch_temp_command_summary: None,
        watch_live_command_summary: None,
        rollback_command_summary: None,
        backup_command_summary: None,
        live_apply_command_summary: None,
        live_verify_command_summary: None,
        live_rollback_command_summary: None,
        pre_activation_gate_verdict_used: None,
        pre_activation_gate_reason_used: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        activation_plan_verdict_used: None,
        activation_plan_reason_used: None,
        guardrail_verdict_used: None,
        guardrail_reason_used: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this drill layer orchestrates bounded activation/apply/watch primitives only; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

impl DrillReport {
    fn with_temp_plan(
        mut self,
        verdict: TinyLiveDrillVerdict,
        reason: String,
        plan: &TempPlanBundle,
        apply_step: Option<DrillStepArtifact>,
        watch_step: Option<DrillStepArtifact>,
        rollback_step: Option<DrillStepArtifact>,
        verification_mismatches: Vec<String>,
    ) -> Self {
        self.verdict = verdict;
        self.reason = reason;
        self.apply_step = apply_step;
        self.watch_step = watch_step;
        self.rollback_step = rollback_step;
        self.apply_command_summary = plan.apply_plan.apply_command_summary.clone();
        self.watch_verify_command_summary = plan.watch_plan.verify_watch_command_summary.clone();
        self.watch_temp_command_summary = plan.watch_plan.watch_temp_command_summary.clone();
        self.rollback_command_summary = plan.apply_plan.rollback_command_summary.clone();
        self.pre_activation_gate_verdict_used = Some(
            plan.watch_plan
                .pre_activation_gate_verdict_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.pre_activation_gate_reason_used = Some(
            plan.watch_plan
                .pre_activation_gate_reason_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.activation_plan_verdict_used = Some(
            plan.watch_plan
                .activation_plan_verdict_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.activation_plan_reason_used = Some(
            plan.watch_plan
                .activation_plan_reason_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.guardrail_verdict_used = Some(
            plan.watch_plan
                .guardrail_verdict_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.guardrail_reason_used = Some(
            plan.watch_plan
                .guardrail_reason_used
                .clone()
                .unwrap_or_default(),
        )
        .filter(|value| !value.is_empty());
        self.verification_mismatches = verification_mismatches;
        self
    }

    fn with_temp_runtime_artifacts(mut self, paths: &DrillPaths, status: &TempDrillStatus) -> Self {
        self.drill_session_path = Some(paths.session_path.display().to_string());
        self.drill_status_path = Some(paths.status_path.display().to_string());
        self.temp_result = Some(serialize_enum(&status.result));
        self
    }
}

fn verify_session_matches_config(
    session: &TempDrillSession,
    config: &Config,
    mismatches: &mut Vec<String>,
) {
    if session.session_version != DRILL_SESSION_VERSION {
        mismatches.push(format!(
            "temp drill session_version must be {}, found {}",
            DRILL_SESSION_VERSION, session.session_version
        ));
    }
    if session.activation_config_path != config.activation_config_path.display().to_string() {
        mismatches.push(
            "temp drill session activation_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.rollback_config_path != config.rollback_config_path.display().to_string() {
        mismatches.push(
            "temp drill session rollback_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.runtime_dir != config.runtime_dir.display().to_string() {
        mismatches.push(
            "temp drill session runtime_dir does not match the explicit runtime dir".to_string(),
        );
    }
}

fn verify_keep_running_drill(
    config: &Config,
    session: &TempDrillSession,
    status: &TempDrillStatus,
    apply_report: Option<&tiny_live_activation_apply::ApplyReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_apply::ApplyReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "keep_running" {
        mismatches.push("temp drill status decision must be keep_running".to_string());
    }
    let Some(apply_report) = apply_report else {
        mismatches.push("missing temp drill apply report".to_string());
        return Ok(());
    };
    let Some(watch_report) = watch_report else {
        mismatches.push("missing temp drill watch report".to_string());
        return Ok(());
    };
    if rollback_report.is_some() {
        mismatches.push(
            "completed_keep_running temp drill must not persist a rollback report".to_string(),
        );
    }
    if apply_report.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
    {
        mismatches.push("temp drill apply report must prove temp_run_started".to_string());
    }
    if watch_report.verdict
        != tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchTempContinue
    {
        mismatches.push("temp drill watch report must prove temp_continue".to_string());
    }
    if watch_report.decision.as_deref() != Some("keep_running") {
        mismatches.push("temp drill watch decision must be keep_running".to_string());
    }
    verify_step_report_paths(session, apply_report, watch_report, mismatches);
    let verify_report = tiny_live_activation_apply::verify_temp_run_for_drill(
        &config.activation_config_path,
        &config.rollback_config_path,
        &config.runtime_dir,
        session.startup_timeout_ms,
        session.rollback_timeout_ms,
    )?;
    if verify_report.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyOk
    {
        mismatches.push(verify_report.reason);
    }
    if let Some(verification) = verify_report.temp_run_verification {
        mismatches.extend(verification.mismatches);
    }
    Ok(())
}

fn verify_rollback_drill(
    config: &Config,
    _session: &TempDrillSession,
    status: &TempDrillStatus,
    apply_report: Option<&tiny_live_activation_apply::ApplyReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_apply::ApplyReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "rollback_completed" {
        mismatches.push("temp drill status decision must be rollback_completed".to_string());
    }
    let Some(apply_report) = apply_report else {
        mismatches.push("missing temp drill apply report".to_string());
        return Ok(());
    };
    let Some(watch_report) = watch_report else {
        mismatches.push("missing temp drill watch report".to_string());
        return Ok(());
    };
    let Some(rollback_report) = rollback_report else {
        mismatches.push("missing temp drill rollback report".to_string());
        return Ok(());
    };
    if apply_report.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
    {
        mismatches.push("temp drill apply report must prove temp_run_started".to_string());
    }
    if watch_report.verdict
        != tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered
    {
        mismatches.push("temp drill watch report must prove temp_rollback_triggered".to_string());
    }
    if rollback_report.verdict
        != tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunCompleted
    {
        mismatches.push("temp drill rollback report must prove rollback_completed".to_string());
    }
    let apply_paths = tiny_live_activation_apply::runtime_paths(&config.runtime_dir);
    let temp_session = tiny_live_activation_apply::load_session(&apply_paths.session_path)?;
    let rollback_status: tiny_live_activation_apply::RollbackStatus =
        load_json(&apply_paths.rollback_status_path)?;
    let verification = tiny_live_activation_apply::verify_rollback_runtime_for_drill(
        &apply_paths,
        &temp_session,
        &rollback_status,
    )?;
    mismatches.extend(verification.mismatches);
    Ok(())
}

fn verify_failed_before_apply_drill(
    config: &Config,
    _session: &TempDrillSession,
    status: &TempDrillStatus,
    apply_report: Option<&tiny_live_activation_apply::ApplyReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_apply::ApplyReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if status.decision != "failed_before_apply" {
        mismatches
            .push("failed_before_apply status must keep decision=failed_before_apply".to_string());
    }
    if rollback_report.is_some() {
        mismatches
            .push("failed_before_apply temp drill must not persist rollback report".to_string());
    }
    let apply_paths = tiny_live_activation_apply::runtime_paths(&config.runtime_dir);
    for path in [
        &apply_paths.session_path,
        &apply_paths.pid_path,
        &apply_paths.status_path,
        &apply_paths.rollback_status_path,
    ] {
        if path.exists() {
            mismatches.push(format!(
                "failed_before_apply temp drill must not leave apply runtime artifact {}",
                path.display()
            ));
        }
    }
    if let Some(apply_report) = apply_report {
        if apply_report.verdict
            == tiny_live_activation_apply::TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
        {
            mismatches.push(
                "failed_before_apply temp drill cannot persist temp_run_started apply verdict"
                    .to_string(),
            );
        }
    }
    if let Some(watch_report) = watch_report {
        if watch_report.verdict == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchTempContinue
            || watch_report.verdict
                == tiny_live_activation_watch::TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered
        {
            mismatches.push(
                "failed_before_apply temp drill cannot persist a temp watch execution verdict"
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn verify_failed_during_watch_drill(
    _config: &Config,
    _session: &TempDrillSession,
    status: &TempDrillStatus,
    apply_report: Option<&tiny_live_activation_apply::ApplyReport>,
    watch_report: Option<&tiny_live_activation_watch::WatchReport>,
    rollback_report: Option<&tiny_live_activation_apply::ApplyReport>,
    mismatches: &mut Vec<String>,
) -> Result<()> {
    if apply_report.is_none() || watch_report.is_none() {
        mismatches.push(
            "failed_during_watch temp drill must still persist apply and watch reports".to_string(),
        );
    }
    if rollback_report.is_none() {
        mismatches.push(
            "failed_during_watch temp drill must persist the rollback attempt evidence".to_string(),
        );
    }
    mismatches.push(format!(
        "temp drill ended in {}; final posture is not safely verifiable",
        status.decision
    ));
    Ok(())
}

fn verify_step_report_paths(
    session: &TempDrillSession,
    apply_report: &tiny_live_activation_apply::ApplyReport,
    watch_report: &tiny_live_activation_watch::WatchReport,
    mismatches: &mut Vec<String>,
) {
    if apply_report.activation_config_path != session.activation_config_path {
        mismatches.push(
            "temp drill apply report activation_config_path does not match session".to_string(),
        );
    }
    if apply_report.rollback_config_path != session.rollback_config_path {
        mismatches.push(
            "temp drill apply report rollback_config_path does not match session".to_string(),
        );
    }
    if watch_report.activation_config_path != session.activation_config_path {
        mismatches.push(
            "temp drill watch report activation_config_path does not match session".to_string(),
        );
    }
    if watch_report.rollback_config_path != session.rollback_config_path {
        mismatches.push(
            "temp drill watch report rollback_config_path does not match session".to_string(),
        );
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanDrill => "plan_drill",
        Mode::RenderDrillScript => "render_drill_script",
        Mode::RunTempDrill => "run_temp_drill",
        Mode::VerifyTempDrill => "verify_temp_drill",
        Mode::PlanLiveDrill => "plan_live_drill",
        Mode::InternalTempRunWorker => "internal_temp_run_worker",
    }
}

fn command_base_for_script(config: &Config) -> Result<String> {
    let executable = env::current_exe()?.display().to_string();
    let mut parts = vec![
        "exec".to_string(),
        shell_quote(&executable),
        "--activation-config".to_string(),
        shell_quote(&config.activation_config_path.display().to_string()),
        "--rollback-config".to_string(),
        shell_quote(&config.rollback_config_path.display().to_string()),
        "--runtime-dir".to_string(),
        shell_quote(&config.runtime_dir.display().to_string()),
    ];
    if let Some(path) = &config.target_config_path {
        parts.push("--target-config".to_string());
        parts.push(shell_quote(&path.display().to_string()));
    }
    if let Some(value) = &config.target_service_name {
        parts.push("--target-service".to_string());
        parts.push(shell_quote(value));
    }
    if let Some(path) = &config.service_control_command_path {
        parts.push("--service-control-command".to_string());
        parts.push(shell_quote(&path.display().to_string()));
    }
    if let Some(path) = &config.backup_dir {
        parts.push("--backup-dir".to_string());
        parts.push(shell_quote(&path.display().to_string()));
    }
    parts.push("--startup-timeout-ms".to_string());
    parts.push(config.startup_timeout_ms.to_string());
    parts.push("--rollback-timeout-ms".to_string());
    parts.push(config.rollback_timeout_ms.to_string());
    parts.push("--watch-window-seconds".to_string());
    parts.push(config.watch_window_seconds.unwrap_or(900).to_string());
    parts.push("--sample-cadence-ms".to_string());
    parts.push(config.sample_cadence_ms.to_string());
    parts.push("--max-observation-staleness-ms".to_string());
    parts.push(
        config
            .max_observation_staleness_ms
            .unwrap_or_else(|| {
                std::cmp::max(
                    MIN_OBSERVATION_STALENESS_MS,
                    config
                        .sample_cadence_ms
                        .saturating_mul(OBSERVATION_STALENESS_MULTIPLIER),
                )
            })
            .to_string(),
    );
    parts.push("--run-temp-drill".to_string());
    parts.push("--json".to_string());
    Ok(parts.join(" "))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("refusing to overwrite existing file {}", path.display());
    }
    Ok(())
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating parent dir for {}", path.display()))?;
        }
    }
    let unique = format!(
        ".tmp-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("cannot derive file name for {}", path.display()))?;
    let temp_path = path.with_file_name(format!("{}{}", file_name.to_string_lossy(), unique));
    fs::write(&temp_path, contents)
        .with_context(|| format!("failed writing temporary file {}", temp_path.display()))?;
    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed replacing {} with temporary file {}",
            path.display(),
            temp_path.display()
        )
    })
}

fn parse_string_arg(label: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {label}"))
}

fn parse_u64_arg(label: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(label, value)?
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {label}"))
}

fn set_mode(slot: &mut Option<Mode>, mode: Mode, flag: &str) -> Result<()> {
    if slot.replace(mode).is_some() {
        bail!("choose exactly one mode flag, found duplicate around {flag}");
    }
    Ok(())
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn render_human(report: &DrillReport) -> String {
    let mut lines = vec![
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("mode={}", report.mode),
        format!("reason={}", report.reason),
        format!("activation_config_path={}", report.activation_config_path),
        format!("rollback_config_path={}", report.rollback_config_path),
        format!("runtime_dir={}", report.runtime_dir),
        format!("startup_timeout_ms={}", report.startup_timeout_ms),
        format!("rollback_timeout_ms={}", report.rollback_timeout_ms),
        format!("watch_window_seconds={}", report.watch_window_seconds),
        format!("sample_cadence_ms={}", report.sample_cadence_ms),
        format!(
            "max_observation_staleness_ms={}",
            report.max_observation_staleness_ms
        ),
    ];
    if let Some(value) = &report.target_config_path {
        lines.push(format!("target_config_path={value}"));
    }
    if let Some(value) = &report.target_service_name {
        lines.push(format!("target_service_name={value}"));
    }
    if let Some(value) = &report.service_control_command_path {
        lines.push(format!("service_control_command_path={value}"));
    }
    if let Some(value) = &report.backup_dir {
        lines.push(format!("backup_dir={value}"));
    }
    if let Some(value) = &report.drill_session_path {
        lines.push(format!("drill_session_path={value}"));
    }
    if let Some(value) = &report.drill_status_path {
        lines.push(format!("drill_status_path={value}"));
    }
    if let Some(value) = &report.temp_result {
        lines.push(format!("temp_result={value}"));
    }
    if let Some(value) = &report.apply_command_summary {
        lines.push(format!("apply_command_summary={value}"));
    }
    if let Some(value) = &report.watch_verify_command_summary {
        lines.push(format!("watch_verify_command_summary={value}"));
    }
    if let Some(value) = &report.watch_temp_command_summary {
        lines.push(format!("watch_temp_command_summary={value}"));
    }
    if let Some(value) = &report.watch_live_command_summary {
        lines.push(format!("watch_live_command_summary={value}"));
    }
    if let Some(value) = &report.rollback_command_summary {
        lines.push(format!("rollback_command_summary={value}"));
    }
    if let Some(value) = &report.backup_command_summary {
        lines.push(format!("backup_command_summary={value}"));
    }
    if let Some(value) = &report.live_apply_command_summary {
        lines.push(format!("live_apply_command_summary={value}"));
    }
    if let Some(value) = &report.live_verify_command_summary {
        lines.push(format!("live_verify_command_summary={value}"));
    }
    if let Some(value) = &report.live_rollback_command_summary {
        lines.push(format!("live_rollback_command_summary={value}"));
    }
    if let Some(value) = &report.pre_activation_gate_verdict_used {
        lines.push(format!("pre_activation_gate_verdict_used={value}"));
    }
    if let Some(value) = &report.pre_activation_gate_reason_used {
        lines.push(format!("pre_activation_gate_reason_used={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_reason {
        lines.push(format!("current_pre_activation_gate_reason={value}"));
    }
    if let Some(value) = &report.activation_plan_verdict_used {
        lines.push(format!("activation_plan_verdict_used={value}"));
    }
    if let Some(value) = &report.activation_plan_reason_used {
        lines.push(format!("activation_plan_reason_used={value}"));
    }
    if let Some(value) = &report.guardrail_verdict_used {
        lines.push(format!("guardrail_verdict_used={value}"));
    }
    if let Some(value) = &report.guardrail_reason_used {
        lines.push(format!("guardrail_reason_used={value}"));
    }
    render_step("apply_step", &report.apply_step, &mut lines);
    render_step("watch_step", &report.watch_step, &mut lines);
    render_step("rollback_step", &report.rollback_step, &mut lines);
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  {entry}")),
        );
    }
    lines.push("activation_authorized=false".to_string());
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
}

fn render_step(label: &str, step: &Option<DrillStepArtifact>, lines: &mut Vec<String>) {
    if let Some(step) = step {
        lines.push(format!("{label}_path={}", step.path));
        lines.push(format!("{label}_mode={}", step.mode));
        lines.push(format!("{label}_verdict={}", step.verdict));
        lines.push(format!("{label}_reason={}", step.reason));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use copybot_config::load_from_path;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn successful_temp_drill_can_complete_keep_running() {
        let fixture = temp_fixture("tiny_live_drill_temp_green");
        let report = run_temp_drill_report_for_tests(&fixture.config).expect("run temp drill");
        assert_eq!(
            report.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillCompletedKeepRunning
        );
        assert_eq!(
            report.temp_result.as_deref(),
            Some("completed_keep_running")
        );
        let verify = verify_temp_drill_report(&fixture.config).expect("verify temp drill");
        assert_eq!(
            verify.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillVerifyOk
        );
        cleanup_temp_runtime(&fixture);
    }

    #[test]
    fn temp_drill_with_rollback_trigger_completes_with_rollback() {
        let fixture = temp_fixture("tiny_live_drill_temp_rollback");
        let report = run_temp_drill_report_for_tests_with_observation(
            &fixture.config,
            &fixture.source_fingerprint_sha256,
            10,
            2,
        )
        .expect("run temp drill");
        assert_eq!(
            report.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillCompletedWithRollback
        );
        assert_eq!(
            report.temp_result.as_deref(),
            Some("completed_with_rollback")
        );
        let verify = verify_temp_drill_report(&fixture.config).expect("verify temp drill");
        assert_eq!(
            verify.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillVerifyOk
        );
    }

    #[test]
    fn temp_drill_failure_before_apply_is_explicit() {
        let fixture = temp_fixture("tiny_live_drill_temp_blocked");
        corrupt_gate_metadata(
            &fixture.activation_config_path,
            "blocked_by_stage3",
            "stage3 blocked",
        );
        let report = run_temp_drill_report_for_tests(&fixture.config).expect("run temp drill");
        assert_eq!(
            report.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillFailedBeforeApply
        );
        assert_eq!(report.temp_result.as_deref(), Some("failed_before_apply"));
    }

    #[test]
    fn verify_temp_drill_catches_missing_watch_report_artifact() {
        let fixture = temp_fixture("tiny_live_drill_temp_verify_missing_watch");
        let report = run_temp_drill_report_for_tests_with_observation(
            &fixture.config,
            &fixture.source_fingerprint_sha256,
            10,
            0,
        )
        .expect("run temp drill");
        assert_eq!(
            report.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillCompletedKeepRunning
        );
        let watch_report_path = drill_paths(&fixture.runtime_dir).watch_report_path;
        fs::remove_file(&watch_report_path).unwrap();
        let verify = verify_temp_drill_report(&fixture.config).expect("verify temp drill");
        assert_eq!(
            verify.verdict,
            TinyLiveDrillVerdict::TinyLiveTempDrillVerifyInvalid
        );
        cleanup_temp_runtime(&fixture);
    }

    #[test]
    fn preexisting_watch_observation_file_blocks_temp_drill_reuse() {
        let fixture = temp_fixture("tiny_live_drill_temp_stale_observation");
        write_watch_observation(
            &fixture.runtime_dir,
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            10,
            0,
        );
        let error = run_temp_drill_report_for_tests(&fixture.config)
            .expect_err("stale observation must block drill reuse");
        let message = error.to_string();
        assert!(message.contains("refusing to reuse runtime dir"));
        assert!(message.contains("tiny_live_activation.watch.observation.json"));

        let drill_paths = drill_paths(&fixture.runtime_dir);
        let runtime_paths = tiny_live_activation_apply::runtime_paths(&fixture.runtime_dir);
        assert!(!drill_paths.session_path.exists());
        assert!(!drill_paths.status_path.exists());
        assert!(!drill_paths.apply_report_path.exists());
        assert!(!drill_paths.watch_report_path.exists());
        assert!(!drill_paths.rollback_report_path.exists());
        assert!(!runtime_paths.session_path.exists());
        assert!(!runtime_paths.pid_path.exists());
        assert!(!runtime_paths.log_path.exists());
        assert!(!runtime_paths.status_path.exists());
        assert!(!runtime_paths.rollback_status_path.exists());
    }

    #[test]
    fn live_drill_plan_is_refused_under_current_blocked_gate_truth() {
        let fixture = live_fixture("tiny_live_drill_live_stage3", GateState::Stage3Blocked);
        let before = fs::read(&fixture.target_config_path).unwrap();
        let report = plan_live_drill_report(&fixture.config).expect("plan live drill");
        assert!(
            matches!(
                report.verdict,
                TinyLiveDrillVerdict::TinyLiveLiveDrillRefusedByStage3
                    | TinyLiveDrillVerdict::TinyLiveLiveDrillRefusedByPreActivationGate
            ),
            "unexpected live drill refusal verdict: {:?}",
            report.verdict
        );
        assert!(report.current_pre_activation_gate_verdict.is_some());
        let after = fs::read(&fixture.target_config_path).unwrap();
        assert_eq!(before, after);
    }

    struct TempFixture {
        _fixture_dir: PathBuf,
        activation_config_path: PathBuf,
        rollback_config_path: PathBuf,
        runtime_dir: PathBuf,
        source_fingerprint_sha256: String,
        config: Config,
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct LiveFixture {
        _fixture_dir: PathBuf,
        target_config_path: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn temp_fixture(label: &str) -> TempFixture {
        let fixture_dir = temp_dir(label);
        let runtime_dir = fixture_dir.join("runtime");
        let activation_config_path = fixture_dir.join("rendered.activation.toml");
        let rollback_config_path = fixture_dir.join("rendered.rollback.toml");
        let source_config_path = fixture_dir.join("source.safe.toml");
        let source_safe = sample_safe_config_toml();
        fs::write(&source_config_path, &source_safe).unwrap();
        let source_loaded = load_from_path(&source_config_path).unwrap();
        let source_fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source_loaded,
        )
        .unwrap();
        write_rendered_artifact(
            &activation_config_path,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            &source_config_path,
            &source_fingerprint.sha256,
            true,
        );
        write_rendered_artifact(
            &rollback_config_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            &source_config_path,
            &source_fingerprint.sha256,
            false,
        );
        TempFixture {
            _fixture_dir: fixture_dir,
            activation_config_path: activation_config_path.clone(),
            rollback_config_path: rollback_config_path.clone(),
            runtime_dir: runtime_dir.clone(),
            source_fingerprint_sha256: source_fingerprint.sha256.clone(),
            config: Config {
                mode: Mode::RunTempDrill,
                activation_config_path,
                rollback_config_path,
                runtime_dir,
                target_config_path: None,
                target_service_name: None,
                service_control_command_path: None,
                backup_dir: None,
                output_path: None,
                startup_timeout_ms: DEFAULT_TEMP_STARTUP_TIMEOUT_MS,
                rollback_timeout_ms: DEFAULT_TEMP_ROLLBACK_TIMEOUT_MS,
                watch_window_seconds: Some(900),
                sample_cadence_ms: 100,
                max_observation_staleness_ms: Some(3_000),
                json: false,
            },
        }
    }

    fn run_temp_drill_report_for_tests(config: &Config) -> Result<DrillReport> {
        run_temp_drill_report_for_tests_with_observation(
            config,
            &load_rendered_source_fingerprint(&config.activation_config_path)?,
            10,
            0,
        )
    }

    fn run_temp_drill_report_for_tests_with_observation(
        config: &Config,
        source_fingerprint_sha256: &str,
        total_execution_attempts: u64,
        execution_error_count: u64,
    ) -> Result<DrillReport> {
        run_temp_drill_report_with_apply_runner(config, |config| {
            let report = tiny_live_activation_apply::apply_temp_run_with_test_worker_for_drill(
                &config.activation_config_path,
                &config.rollback_config_path,
                &config.runtime_dir,
                config.startup_timeout_ms,
                config.rollback_timeout_ms,
            )?;
            write_watch_observation(
                &config.runtime_dir,
                source_fingerprint_sha256,
                Utc::now() - ChronoDuration::minutes(16),
                Utc::now(),
                total_execution_attempts,
                execution_error_count,
            );
            Ok(report)
        })
    }

    fn load_rendered_source_fingerprint(path: &Path) -> Result<String> {
        Ok(
            tiny_live_activation_execute::inspect_rendered_config_artifact(path)?
                .metadata
                .source_config_fingerprint_sha256,
        )
    }

    fn cleanup_temp_runtime(fixture: &TempFixture) {
        let _ = tiny_live_activation_apply::rollback_temp_run_for_drill(
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            &fixture.runtime_dir,
            fixture.config.startup_timeout_ms,
            fixture.config.rollback_timeout_ms,
        );
    }

    fn live_fixture(label: &str, gate_state: GateState) -> LiveFixture {
        let fixture_dir = temp_dir(label);
        let runtime_dir = fixture_dir.join("runtime");
        let backup_dir = fixture_dir.join("backups");
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
        write_live_rendered_artifact_pair(
            &target_config_path,
            &activation_config_path,
            &rollback_config_path,
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
        );
        LiveFixture {
            _fixture_dir: fixture_dir,
            target_config_path: target_config_path.clone(),
            config: Config {
                mode: Mode::PlanLiveDrill,
                activation_config_path,
                rollback_config_path,
                runtime_dir,
                target_config_path: Some(target_config_path),
                target_service_name: Some("solana-copy-bot.service".to_string()),
                service_control_command_path: Some(service_control_command_path),
                backup_dir: Some(backup_dir),
                output_path: None,
                startup_timeout_ms: DEFAULT_LIVE_STARTUP_TIMEOUT_MS,
                rollback_timeout_ms: DEFAULT_LIVE_ROLLBACK_TIMEOUT_MS,
                watch_window_seconds: Some(900),
                sample_cadence_ms: 100,
                max_observation_staleness_ms: Some(3_000),
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn write_watch_observation(
        runtime_dir: &Path,
        source_fingerprint_sha256: &str,
        sample_window_started_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        total_execution_attempts: u64,
        execution_error_count: u64,
    ) {
        fs::create_dir_all(runtime_dir).unwrap();
        let observation = json!({
            "observation_version": "1",
            "watch_target_kind": "temp_run",
            "observed_at": observed_at,
            "sample_window_started_at": sample_window_started_at,
            "runtime_dir": runtime_dir.display().to_string(),
            "activation_config_path": runtime_dir.parent().unwrap().join("rendered.activation.toml").display().to_string(),
            "rollback_config_path": runtime_dir.parent().unwrap().join("rendered.rollback.toml").display().to_string(),
            "target_config_path": null,
            "target_service_name": null,
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
            runtime_dir.join("tiny_live_activation.watch.observation.json"),
            serde_json::to_string_pretty(&observation).unwrap(),
        )
        .unwrap();
    }

    fn write_rendered_artifact(
        path: &Path,
        render_kind: tiny_live_activation_execute::RenderKind,
        contents: String,
        source_config_path: &Path,
        source_fingerprint_sha256: &str,
        activation_enabled: bool,
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
                source_value: json!(!activation_enabled),
                target_value: json!(activation_enabled),
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

    fn corrupt_gate_metadata(path: &Path, verdict: &str, reason: &str) {
        let metadata_path = tiny_live_activation_execute::metadata_path_for_rendered_config(path);
        let mut metadata: tiny_live_activation_execute::RenderedConfigMetadata =
            serde_json::from_str(&fs::read_to_string(&metadata_path).unwrap()).unwrap();
        metadata.pre_activation_gate_verdict = verdict.to_string();
        metadata.pre_activation_gate_reason = reason.to_string();
        fs::write(
            metadata_path,
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
    }

    fn write_live_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
    ) {
        let source = load_from_path(target_config_path).unwrap();
        let source_fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source,
        )
        .unwrap();
        write_live_rendered_artifact(
            activation_path,
            tiny_live_activation_execute::RenderKind::Activation,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, true),
            target_config_path,
            &source_fingerprint.sha256,
            false,
            true,
        );
        write_live_rendered_artifact(
            rollback_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, false),
            target_config_path,
            &source_fingerprint.sha256,
            false,
            false,
        );
    }

    fn write_live_rendered_artifact(
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
            GateState::Green | GateState::PreActivationBlocked => {
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

    fn write_fake_service_control_command(path: &Path) {
        let script = "#!/usr/bin/env bash\nset -euo pipefail\nstatus_path=\"$1\"\naction=\"$2\"\nservice_name=\"$3\"\ntarget_config=\"$4\"\nexpected_fp=\"$5\"\nexpected_enabled=\"$6\"\ncat > \"$status_path\" <<JSON\n{\n  \"status_version\": \"1\",\n  \"action\": \"$action\",\n  \"observed_at\": \"2026-03-27T12:00:00Z\",\n  \"service_name\": \"$service_name\",\n  \"target_config_path\": \"$target_config\",\n  \"expected_config_fingerprint_sha256\": \"$expected_fp\",\n  \"observed_config_fingerprint_sha256\": \"$expected_fp\",\n  \"expected_execution_enabled\": $expected_enabled,\n  \"observed_execution_enabled\": $expected_enabled,\n  \"restart_successful\": true,\n  \"note\": \"fake service control\"\n}\nJSON\n";
        fs::write(path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
    }

    struct MockHttpServer {
        handle: Option<std::thread::JoinHandle<()>>,
        addr: std::net::SocketAddr,
        shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockHttpServer {
        fn spawn(
            max_requests: usize,
            response_body: String,
            required_header: Option<&str>,
        ) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let required_header = required_header.map(str::to_string);
            let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let worker_shutdown = shutdown.clone();
            let handle = std::thread::spawn(move || {
                let mut served = 0usize;
                while served < max_requests
                    && !worker_shutdown.load(std::sync::atomic::Ordering::Relaxed)
                {
                    let (mut stream, _) = match listener.accept() {
                        Ok(value) => value,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            continue;
                        }
                        Err(_) => break,
                    };
                    use std::io::{Read, Write};
                    let mut buffer = [0_u8; 4096];
                    let bytes = stream.read(&mut buffer).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buffer[..bytes]);
                    if let Some(header) = &required_header {
                        if !request.to_ascii_lowercase().contains(header) {
                            let body = "{\"error\":\"missing auth\"}";
                            let response = format!(
                                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(response.as_bytes());
                            served += 1;
                            continue;
                        }
                    }
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    served += 1;
                }
            });
            Self {
                handle: Some(handle),
                addr,
                shutdown,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}:{}{}", self.addr.ip(), self.addr.port(), path)
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            self.shutdown
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = std::net::TcpStream::connect(self.addr);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn sample_activation_config_toml() -> String {
        r#"
[system]
env = "prod-live"

[storage]
db_path = "/tmp/copybot-live.db"

[execution]
enabled = true
mode = "adapter_submit_confirm"
batch_size = 1
default_route = "jito"
rpc_http_url = "https://rpc.example"
submit_adapter_http_url = "http://127.0.0.1:8080/submit"
submit_adapter_require_policy_echo = true
submit_allowed_routes = ["jito"]
submit_route_order = ["jito"]
pretrade_max_fee_overhead_bps = 800
pretrade_max_priority_fee_lamports = 1500
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

    fn sample_safe_config_toml() -> String {
        sample_activation_config_toml().replacen("enabled = true", "enabled = false", 1)
    }

    fn sample_rollback_config_toml() -> String {
        sample_safe_config_toml()
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}_{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
