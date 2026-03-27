use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "copybot_activation_decision_packet.rs"]
pub(crate) mod activation_decision_packet;
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
#[path = "copybot_tiny_live_guardrail_audit.rs"]
pub(crate) mod tiny_live_guardrail_audit;

const USAGE: &str = "usage: copybot_tiny_live_activation_watch --activation-config <path> --rollback-config <path> --runtime-dir <path> [--target-config <path>] [--target-service <name>] [--service-control-command <path>] [--backup-dir <path>] [--output <path>] [--json] [--watch-window-seconds <seconds>] [--sample-cadence-ms <ms>] [--max-observation-staleness-ms <ms>] (--plan-watch | --render-watch-script | --verify-watch-target | --watch-temp-run | --watch-live-target)";
const OBSERVATION_VERSION: &str = "1";
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
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanWatch,
    RenderWatchScript,
    VerifyWatchTarget,
    WatchTempRun,
    WatchLiveTarget,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TinyLiveWatchVerdict {
    TinyLiveWatchPlanReady,
    TinyLiveWatchScriptRendered,
    TinyLiveWatchVerifyOk,
    TinyLiveWatchVerifyInvalid,
    TinyLiveWatchTempContinue,
    TinyLiveWatchTempRollbackTriggered,
    TinyLiveWatchLiveContinue,
    TinyLiveWatchLiveRollbackTriggered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchTargetKind {
    TempRun,
    LiveTarget,
}

impl WatchTargetKind {
    fn as_str(self) -> &'static str {
        match self {
            WatchTargetKind::TempRun => "temp_run",
            WatchTargetKind::LiveTarget => "live_target",
        }
    }
}

#[derive(Debug, Clone)]
struct ValidatedArtifactPair {
    activation: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
    rollback: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
}

#[derive(Debug, Clone)]
struct WatchContext {
    pair: ValidatedArtifactPair,
    guardrail: tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
    watch_window_seconds: u64,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: u64,
    verify_watch_command_summary: String,
    watch_temp_command_summary: String,
    watch_live_command_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WatchObservation {
    observation_version: String,
    watch_target_kind: String,
    observed_at: DateTime<Utc>,
    sample_window_started_at: DateTime<Utc>,
    runtime_dir: String,
    activation_config_path: String,
    rollback_config_path: String,
    target_config_path: Option<String>,
    target_service_name: Option<String>,
    source_config_fingerprint_sha256: String,
    target_running: bool,
    total_execution_attempts: u64,
    execution_error_count: u64,
    adapter_contract_failure_count: u64,
    policy_echo_mismatch_count: u64,
    fee_or_slippage_breach_count: u64,
    connectivity_degraded_seconds: u64,
    daily_realized_loss_sol: f64,
    consecutive_hard_failures: u32,
    note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ObservationEvaluationSummary {
    observation_path: String,
    observation_present: bool,
    observation_fresh: Option<bool>,
    target_running: Option<bool>,
    sample_window_seconds: Option<u64>,
    window_complete: bool,
    total_execution_attempts: Option<u64>,
    execution_error_rate_pct: Option<f64>,
    adapter_contract_failure_rate_pct: Option<f64>,
    policy_echo_mismatch_rate_pct: Option<f64>,
    fee_or_slippage_breach_rate_pct: Option<f64>,
    connectivity_degraded_seconds: Option<u64>,
    daily_realized_loss_sol: Option<f64>,
    consecutive_hard_failures: Option<u32>,
    bounded_but_degraded: bool,
    keep_running: bool,
    rollback_triggered: bool,
    warnings: Vec<String>,
    mismatches: Vec<String>,
    rollback_triggers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveWatchVerificationSummary {
    service_status_artifact_present: bool,
    backup_proof_present: bool,
    current_target_fingerprint_sha256: Option<String>,
    current_target_execution_enabled: Option<bool>,
    service_status_observed_at: Option<DateTime<Utc>>,
    service_status_fresh: Option<bool>,
    service_status_age_ms: Option<u64>,
    max_service_status_staleness_ms: u64,
    mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WatchReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) mode: String,
    pub(crate) verdict: TinyLiveWatchVerdict,
    pub(crate) reason: String,
    pub(crate) activation_config_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) activation_metadata_path: Option<String>,
    pub(crate) rollback_metadata_path: Option<String>,
    pub(crate) runtime_dir: String,
    pub(crate) target_config_path: Option<String>,
    pub(crate) target_service_name: Option<String>,
    pub(crate) service_control_command_path: Option<String>,
    pub(crate) backup_dir: Option<String>,
    pub(crate) watch_window_seconds: u64,
    pub(crate) sample_cadence_ms: u64,
    pub(crate) max_observation_staleness_ms: u64,
    pub(crate) watch_target_kind: Option<String>,
    pub(crate) decision: Option<String>,
    pub(crate) bounded_but_degraded: bool,
    pub(crate) pre_activation_gate_verdict_used: Option<String>,
    pub(crate) pre_activation_gate_reason_used: Option<String>,
    pub(crate) activation_plan_verdict_used: Option<String>,
    pub(crate) activation_plan_reason_used: Option<String>,
    pub(crate) guardrail_verdict_used: Option<String>,
    pub(crate) guardrail_reason_used: Option<String>,
    pub(crate) expected_rollback_triggers: Vec<String>,
    pub(crate) verify_watch_command_summary: Option<String>,
    pub(crate) watch_temp_command_summary: Option<String>,
    pub(crate) watch_live_command_summary: Option<String>,
    pub(crate) temp_runtime_verification:
        Option<tiny_live_activation_apply::TempRunVerificationSummary>,
    pub(crate) live_watch_verification: Option<LiveWatchVerificationSummary>,
    pub(crate) observation_evaluation: Option<ObservationEvaluationSummary>,
    pub(crate) activation_authorized: bool,
    pub(crate) explicit_statement: String,
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
            "--plan-watch" => set_mode(&mut mode, Mode::PlanWatch, "--plan-watch")?,
            "--render-watch-script" => {
                set_mode(&mut mode, Mode::RenderWatchScript, "--render-watch-script")?
            }
            "--verify-watch-target" => {
                set_mode(&mut mode, Mode::VerifyWatchTarget, "--verify-watch-target")?
            }
            "--watch-temp-run" => set_mode(&mut mode, Mode::WatchTempRun, "--watch-temp-run")?,
            "--watch-live-target" => {
                set_mode(&mut mode, Mode::WatchLiveTarget, "--watch-live-target")?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| {
        anyhow!(
            "select exactly one mode: --plan-watch | --render-watch-script | --verify-watch-target | --watch-temp-run | --watch-live-target"
        )
    })?;
    let activation_config_path =
        activation_config_path.ok_or_else(|| anyhow!("missing required --activation-config"))?;
    let rollback_config_path =
        rollback_config_path.ok_or_else(|| anyhow!("missing required --rollback-config"))?;
    let runtime_dir = runtime_dir.ok_or_else(|| anyhow!("missing required --runtime-dir"))?;
    if matches!(mode, Mode::RenderWatchScript) && output_path.is_none() {
        bail!("--output is required for --render-watch-script");
    }
    if !matches!(mode, Mode::RenderWatchScript) && output_path.is_some() {
        bail!("--output is only valid with --render-watch-script");
    }
    if matches!(mode, Mode::VerifyWatchTarget | Mode::WatchLiveTarget) {
        if target_config_path.is_none() {
            bail!("missing required --target-config for live-target watch modes");
        }
        if target_service_name.is_none() {
            bail!("missing required --target-service for live-target watch modes");
        }
        if service_control_command_path.is_none() {
            bail!("missing required --service-control-command for live-target watch modes");
        }
        if backup_dir.is_none() {
            bail!("missing required --backup-dir for live-target watch modes");
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
        watch_window_seconds,
        sample_cadence_ms: sample_cadence_ms.max(MIN_SAMPLE_CADENCE_MS),
        max_observation_staleness_ms,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanWatch => build_plan_watch_report(&config),
        Mode::RenderWatchScript => render_watch_script_report(&config),
        Mode::VerifyWatchTarget => verify_watch_target_report(&config),
        Mode::WatchTempRun => watch_temp_run_report(&config),
        Mode::WatchLiveTarget => watch_live_target_report(&config),
    }?;
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

fn build_plan_watch_report(config: &Config) -> Result<WatchReport> {
    let context = match build_context(config) {
        Ok(value) => value,
        Err(error) => {
            return Ok(invalid_report(
                config,
                "plan_watch",
                error.to_string(),
                None,
                None,
                None,
            ))
        }
    };
    Ok(WatchReport {
        generated_at: Utc::now(),
        mode: "plan_watch".to_string(),
        verdict: TinyLiveWatchVerdict::TinyLiveWatchPlanReady,
        reason: "bounded post-activation watch contract is explicit; continue-vs-rollback evaluation still remains non-authorizing and separate from any production activation decision".to_string(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(context.pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(context.pair.rollback.metadata_path.display().to_string()),
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
        backup_dir: config
            .backup_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        watch_window_seconds: context.watch_window_seconds,
        sample_cadence_ms: context.sample_cadence_ms,
        max_observation_staleness_ms: context.max_observation_staleness_ms,
        watch_target_kind: None,
        decision: None,
        bounded_but_degraded: false,
        pre_activation_gate_verdict_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_verdict
                .clone(),
        ),
        pre_activation_gate_reason_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_reason
                .clone(),
        ),
        activation_plan_verdict_used: Some(
            context.pair.activation.metadata.activation_plan_verdict.clone(),
        ),
        activation_plan_reason_used: Some(
            context.pair.activation.metadata.activation_plan_reason.clone(),
        ),
        guardrail_verdict_used: Some(serialize_enum(&context.guardrail.verdict)),
        guardrail_reason_used: Some(context.guardrail.reason.clone()),
        expected_rollback_triggers: expected_rollback_triggers(&context.guardrail),
        verify_watch_command_summary: Some(context.verify_watch_command_summary),
        watch_temp_command_summary: Some(context.watch_temp_command_summary),
        watch_live_command_summary: context.watch_live_command_summary,
        temp_runtime_verification: None,
        live_watch_verification: None,
        observation_evaluation: None,
        activation_authorized: false,
        explicit_statement:
            "this watch layer only supervises bounded post-activation health and rollback triggers; it does not authorize or perform production activation by itself"
                .to_string(),
    })
}

fn render_watch_script_report(config: &Config) -> Result<WatchReport> {
    let plan = build_plan_watch_report(config)?;
    if plan.verdict != TinyLiveWatchVerdict::TinyLiveWatchPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-watch-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nexec {} \"$@\"\n",
        command_base_for_script(config, &plan)?
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(WatchReport {
        generated_at: Utc::now(),
        mode: "render_watch_script".to_string(),
        verdict: TinyLiveWatchVerdict::TinyLiveWatchScriptRendered,
        reason: "bounded post-activation watch wrapper script rendered".to_string(),
        explicit_statement:
            "rendered watch script only wraps bounded watch commands; it does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn verify_watch_target_report(config: &Config) -> Result<WatchReport> {
    let context = match build_context(config) {
        Ok(value) => value,
        Err(error) => {
            return Ok(invalid_report(
                config,
                "verify_watch_target",
                error.to_string(),
                None,
                None,
                None,
            ))
        }
    };
    let live_summary =
        match verify_live_contract(config, &context.pair, context.max_observation_staleness_ms) {
            Ok(value) => value,
            Err(error) => {
                return Ok(invalid_report(
                    config,
                    "verify_watch_target",
                    error.to_string(),
                    Some(&context),
                    None,
                    None,
                ))
            }
        };
    let observation =
        evaluate_observation_once(config, &context, WatchTargetKind::LiveTarget, true)?;
    let mut mismatches = live_summary.mismatches.clone();
    mismatches.extend(observation.mismatches.clone());
    let verdict = if mismatches.is_empty() && !observation.rollback_triggered {
        TinyLiveWatchVerdict::TinyLiveWatchVerifyOk
    } else {
        TinyLiveWatchVerdict::TinyLiveWatchVerifyInvalid
    };
    let reason = if verdict == TinyLiveWatchVerdict::TinyLiveWatchVerifyOk {
        if observation.bounded_but_degraded {
            "watch target remains bounded and non-triggered, but current evidence is still degraded or incomplete".to_string()
        } else {
            "watch target contract is valid and current bounded observation remains inside rollback thresholds".to_string()
        }
    } else if let Some(reason) = mismatches
        .first()
        .cloned()
        .or_else(|| observation.rollback_triggers.first().cloned())
    {
        reason
    } else {
        "watch target verification failed".to_string()
    };
    Ok(WatchReport {
        generated_at: Utc::now(),
        mode: "verify_watch_target".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(context.pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(context.pair.rollback.metadata_path.display().to_string()),
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
        watch_window_seconds: context.watch_window_seconds,
        sample_cadence_ms: context.sample_cadence_ms,
        max_observation_staleness_ms: context.max_observation_staleness_ms,
        watch_target_kind: Some(WatchTargetKind::LiveTarget.as_str().to_string()),
        decision: Some(if verdict == TinyLiveWatchVerdict::TinyLiveWatchVerifyOk {
            "keep_running".to_string()
        } else {
            "rollback_now".to_string()
        }),
        bounded_but_degraded: observation.bounded_but_degraded,
        pre_activation_gate_verdict_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_verdict
                .clone(),
        ),
        pre_activation_gate_reason_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_reason
                .clone(),
        ),
        activation_plan_verdict_used: Some(
            context.pair.activation.metadata.activation_plan_verdict.clone(),
        ),
        activation_plan_reason_used: Some(
            context.pair.activation.metadata.activation_plan_reason.clone(),
        ),
        guardrail_verdict_used: Some(serialize_enum(&context.guardrail.verdict)),
        guardrail_reason_used: Some(context.guardrail.reason.clone()),
        expected_rollback_triggers: expected_rollback_triggers(&context.guardrail),
        verify_watch_command_summary: Some(context.verify_watch_command_summary),
        watch_temp_command_summary: Some(context.watch_temp_command_summary),
        watch_live_command_summary: context.watch_live_command_summary,
        temp_runtime_verification: None,
        live_watch_verification: Some(live_summary),
        observation_evaluation: Some(observation),
        activation_authorized: false,
        explicit_statement:
            "verify-watch-target validates bounded post-activation supervision inputs only; it does not authorize or perform production activation"
                .to_string(),
    })
}

fn watch_temp_run_report(config: &Config) -> Result<WatchReport> {
    let context = match build_context(config) {
        Ok(value) => value,
        Err(error) => {
            return Ok(invalid_report(
                config,
                "watch_temp_run",
                error.to_string(),
                None,
                None,
                None,
            ))
        }
    };
    if let Err(error) = tiny_live_activation_apply::validate_runtime_dir(&config.runtime_dir) {
        return Ok(invalid_report(
            config,
            "watch_temp_run",
            error.to_string(),
            Some(&context),
            None,
            None,
        ));
    }
    let paths = tiny_live_activation_apply::runtime_paths(&config.runtime_dir);
    let session = match tiny_live_activation_apply::load_session(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::TempRun,
                TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered,
                error.to_string(),
                false,
                None,
                None,
                None,
            ))
        }
    };
    if let Err(reason) = verify_temp_session_matches_config(&session, config, &context.pair) {
        return Ok(watch_decision_report(
            config,
            &context,
            WatchTargetKind::TempRun,
            TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered,
            reason,
            false,
            None,
            None,
            None,
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(context.watch_window_seconds);
    loop {
        let verification = match tiny_live_activation_apply::verify_temp_runtime(&paths, &session) {
            Ok(value) => value,
            Err(error) => {
                return Ok(watch_decision_report(
                    config,
                    &context,
                    WatchTargetKind::TempRun,
                    TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered,
                    error.to_string(),
                    false,
                    Some(default_missing_observation_summary(config, &context)),
                    Some(default_temp_verification_failure(error.to_string())),
                    None,
                ))
            }
        };
        if let Some(reason) = verification.mismatches.first().cloned() {
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::TempRun,
                TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered,
                reason,
                false,
                Some(default_missing_observation_summary(config, &context)),
                Some(verification),
                None,
            ));
        }
        let observation =
            evaluate_observation_once(config, &context, WatchTargetKind::TempRun, false)?;
        if observation.rollback_triggered {
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::TempRun,
                TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered,
                observation
                    .rollback_triggers
                    .first()
                    .cloned()
                    .or_else(|| observation.mismatches.first().cloned())
                    .unwrap_or_else(|| "temp watch triggered rollback".to_string()),
                observation.bounded_but_degraded,
                Some(observation),
                Some(verification),
                None,
            ));
        }
        if observation.window_complete {
            let reason = if observation.bounded_but_degraded {
                "bounded temp watch window completed without rollback triggers, but evidence remains degraded or incomplete".to_string()
            } else {
                "bounded temp watch window completed without rollback triggers; keep-running remains inside the accepted guardrail envelope".to_string()
            };
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::TempRun,
                TinyLiveWatchVerdict::TinyLiveWatchTempContinue,
                reason,
                observation.bounded_but_degraded,
                Some(observation),
                Some(verification),
                None,
            ));
        }
        if Instant::now() >= deadline {
            let reason = if observation.observation_present {
                "bounded temp watch window ended before a full guardrail sample completed, but no rollback trigger breached the accepted envelope".to_string()
            } else {
                "temp watch observation artifact is still missing when the bounded watch window expired".to_string()
            };
            let verdict = if observation.observation_present {
                TinyLiveWatchVerdict::TinyLiveWatchTempContinue
            } else {
                TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered
            };
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::TempRun,
                verdict,
                reason,
                true,
                Some(observation),
                Some(verification),
                None,
            ));
        }
        thread::sleep(Duration::from_millis(context.sample_cadence_ms));
    }
}

fn watch_live_target_report(config: &Config) -> Result<WatchReport> {
    let context = match build_context(config) {
        Ok(value) => value,
        Err(error) => {
            return Ok(invalid_report(
                config,
                "watch_live_target",
                error.to_string(),
                None,
                None,
                None,
            ))
        }
    };
    let deadline = Instant::now() + Duration::from_secs(context.watch_window_seconds);
    loop {
        let live_summary =
            match verify_live_contract(config, &context.pair, context.max_observation_staleness_ms)
            {
                Ok(value) => value,
                Err(error) => {
                    return Ok(watch_decision_report(
                        config,
                        &context,
                        WatchTargetKind::LiveTarget,
                        TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered,
                        error.to_string(),
                        false,
                        Some(default_missing_observation_summary(config, &context)),
                        None,
                        None,
                    ))
                }
            };
        if let Some(reason) = live_summary.mismatches.first().cloned() {
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::LiveTarget,
                TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered,
                reason,
                false,
                Some(default_missing_observation_summary(config, &context)),
                None,
                Some(live_summary),
            ));
        }
        let observation =
            evaluate_observation_once(config, &context, WatchTargetKind::LiveTarget, false)?;
        if observation.rollback_triggered {
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::LiveTarget,
                TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered,
                observation
                    .rollback_triggers
                    .first()
                    .cloned()
                    .or_else(|| observation.mismatches.first().cloned())
                    .unwrap_or_else(|| "live-target watch triggered rollback".to_string()),
                observation.bounded_but_degraded,
                Some(observation),
                None,
                Some(live_summary),
            ));
        }
        if observation.window_complete {
            let reason = if observation.bounded_but_degraded {
                "bounded live-target watch window completed without rollback triggers, but evidence remains degraded or incomplete".to_string()
            } else {
                "bounded live-target watch window completed without rollback triggers; keep-running remains inside the accepted guardrail envelope".to_string()
            };
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::LiveTarget,
                TinyLiveWatchVerdict::TinyLiveWatchLiveContinue,
                reason,
                observation.bounded_but_degraded,
                Some(observation),
                None,
                Some(live_summary),
            ));
        }
        if Instant::now() >= deadline {
            let reason = if observation.observation_present {
                "bounded live-target watch window ended before a full guardrail sample completed, but no rollback trigger breached the accepted envelope".to_string()
            } else {
                "live-target watch observation artifact is still missing when the bounded watch window expired".to_string()
            };
            let verdict = if observation.observation_present {
                TinyLiveWatchVerdict::TinyLiveWatchLiveContinue
            } else {
                TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered
            };
            return Ok(watch_decision_report(
                config,
                &context,
                WatchTargetKind::LiveTarget,
                verdict,
                reason,
                true,
                Some(observation),
                None,
                Some(live_summary),
            ));
        }
        thread::sleep(Duration::from_millis(context.sample_cadence_ms));
    }
}

fn build_context(config: &Config) -> Result<WatchContext> {
    let pair = load_validated_artifact_pair(config)?;
    validate_watch_pair_contract(&pair)?;
    let guardrail = tiny_live_guardrail_audit::evaluate_tiny_live_guardrails(
        &config.activation_config_path,
        &pair.activation.rendered_config,
    )?;
    if guardrail.verdict
        != tiny_live_guardrail_audit::TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded
    {
        bail!(
            "activation artifact does not carry a bounded tiny-live guardrail contract: {}",
            guardrail.reason
        );
    }
    let watch_window_seconds = config
        .watch_window_seconds
        .unwrap_or_else(|| guardrail.evaluation_window_seconds.max(1));
    let max_observation_staleness_ms = config.max_observation_staleness_ms.unwrap_or_else(|| {
        std::cmp::max(
            MIN_OBSERVATION_STALENESS_MS,
            config
                .sample_cadence_ms
                .saturating_mul(OBSERVATION_STALENESS_MULTIPLIER),
        )
    });
    let summaries =
        build_command_summaries(config, watch_window_seconds, max_observation_staleness_ms)?;
    Ok(WatchContext {
        pair,
        guardrail,
        watch_window_seconds,
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms,
        verify_watch_command_summary: summaries.0,
        watch_temp_command_summary: summaries.1,
        watch_live_command_summary: summaries.2,
    })
}

fn load_validated_artifact_pair(config: &Config) -> Result<ValidatedArtifactPair> {
    let activation = tiny_live_activation_execute::inspect_rendered_config_artifact(
        &config.activation_config_path,
    )
    .with_context(|| {
        format!(
            "activation rendered artifact {} is invalid",
            config.activation_config_path.display()
        )
    })?;
    let rollback = tiny_live_activation_execute::inspect_rendered_config_artifact(
        &config.rollback_config_path,
    )
    .with_context(|| {
        format!(
            "rollback rendered artifact {} is invalid",
            config.rollback_config_path.display()
        )
    })?;
    Ok(ValidatedArtifactPair {
        activation,
        rollback,
    })
}

fn validate_watch_pair_contract(pair: &ValidatedArtifactPair) -> Result<()> {
    if pair.activation.metadata.render_kind != tiny_live_activation_execute::RenderKind::Activation
    {
        bail!("activation artifact must have render_kind=activation");
    }
    if pair.rollback.metadata.render_kind != tiny_live_activation_execute::RenderKind::Rollback {
        bail!("rollback artifact must have render_kind=rollback");
    }
    if !pair.activation.rendered_config.execution.enabled {
        bail!("activation artifact must keep execution.enabled=true");
    }
    if pair.rollback.rendered_config.execution.enabled {
        bail!("rollback artifact must keep execution.enabled=false");
    }
    if pair.activation.metadata.source_config_fingerprint_sha256
        != pair.rollback.metadata.source_config_fingerprint_sha256
    {
        bail!("activation and rollback artifacts must share the same source config fingerprint");
    }
    if pair.activation.metadata.source_config_fingerprint_scope
        != pair.rollback.metadata.source_config_fingerprint_scope
    {
        bail!(
            "activation and rollback artifacts must share the same source config fingerprint scope"
        );
    }
    if pair.activation.metadata.input_config_path != pair.rollback.metadata.input_config_path {
        bail!("activation and rollback artifacts must point back to the same source config path");
    }
    if pair.activation.metadata.activation_plan_verdict
        != "activation_plan_ready_when_stage_gate_allows"
    {
        bail!(
            "activation artifact does not carry a ready bounded activation-plan verdict: {}",
            pair.activation.metadata.activation_plan_verdict
        );
    }
    if !pair.activation.metadata.activation_overlay_complete {
        bail!("activation artifact metadata must keep activation_overlay_complete=true");
    }
    if !pair.activation.metadata.rollback_plan_complete
        || !pair.rollback.metadata.rollback_plan_complete
    {
        bail!("activation and rollback artifacts must both keep rollback_plan_complete=true");
    }
    if !pair.activation.metadata.service_restart_contract_complete
        || !pair.rollback.metadata.service_restart_contract_complete
    {
        bail!("activation and rollback artifacts must both keep service_restart_contract_complete=true");
    }
    Ok(())
}

fn verify_temp_session_matches_config(
    session: &tiny_live_activation_apply::TempRunSession,
    config: &Config,
    pair: &ValidatedArtifactPair,
) -> std::result::Result<(), String> {
    if session.activation_config_path != config.activation_config_path.display().to_string() {
        return Err(
            "temp watch session activation_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.rollback_config_path != config.rollback_config_path.display().to_string() {
        return Err(
            "temp watch session rollback_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.activation_metadata_path != pair.activation.metadata_path.display().to_string() {
        return Err(
            "temp watch session activation_metadata_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.rollback_metadata_path != pair.rollback.metadata_path.display().to_string() {
        return Err(
            "temp watch session rollback_metadata_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if session.source_config_fingerprint_sha256
        != pair.activation.metadata.source_config_fingerprint_sha256
    {
        return Err(
            "temp watch session source_config_fingerprint_sha256 does not match the activation artifact"
                .to_string(),
        );
    }
    Ok(())
}

fn verify_live_contract(
    config: &Config,
    pair: &ValidatedArtifactPair,
    max_service_status_staleness_ms: u64,
) -> Result<LiveWatchVerificationSummary> {
    let target_config_path = config
        .target_config_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --target-config for live watch"))?;
    let target_service_name = config
        .target_service_name
        .as_ref()
        .ok_or_else(|| anyhow!("missing --target-service for live watch"))?;
    let service_control_command_path = config
        .service_control_command_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --service-control-command for live watch"))?;
    let backup_dir = config
        .backup_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --backup-dir for live watch"))?;

    let target = tiny_live_activation_live_execute::inspect_target_contract_for_watch(
        &config.activation_config_path,
        &config.rollback_config_path,
        target_config_path,
        target_service_name,
        service_control_command_path,
        &config.runtime_dir,
        backup_dir,
    )?;
    let activation_fingerprint = activation_decision_packet::build_config_fingerprint(
        tiny_live_activation_execute::FINGERPRINT_SCOPE,
        &pair.activation.rendered_config,
    )?;
    let current_target = load_from_path(target_config_path).with_context(|| {
        format!(
            "failed loading live target config {} during watch verification",
            target_config_path.display()
        )
    })?;
    let mut mismatches = Vec::new();
    if target.current_fingerprint.sha256 != activation_fingerprint.sha256 {
        mismatches.push(format!(
            "current live target fingerprint {} does not match activation artifact fingerprint {}",
            target.current_fingerprint.sha256, activation_fingerprint.sha256
        ));
    }
    if !current_target.execution.enabled {
        mismatches.push(
            "current live target config must keep execution.enabled=true while tiny-live watch is active"
                .to_string(),
        );
    }
    let backup_proof_present = match tiny_live_activation_live_execute::load_backup_proof_for_watch(
        &config.activation_config_path,
        &config.rollback_config_path,
        target_config_path,
        target_service_name,
        service_control_command_path,
        &config.runtime_dir,
        backup_dir,
    ) {
        Ok(_) => true,
        Err(error) => {
            mismatches.push(error.to_string());
            false
        }
    };
    let runtime_paths = tiny_live_activation_live_execute::runtime_paths(&config.runtime_dir);
    let service_status_artifact_present = runtime_paths.apply_status_path.exists();
    let mut service_status_observed_at = None;
    let mut service_status_fresh = None;
    let mut service_status_age_ms = None;
    if service_status_artifact_present {
        let status = tiny_live_activation_live_execute::load_live_service_status(
            &runtime_paths.apply_status_path,
        )
        .with_context(|| {
            format!(
                "failed reading live activation status artifact {}",
                runtime_paths.apply_status_path.display()
            )
        })?;
        service_status_observed_at = Some(status.observed_at);
        let age_ms = observed_age_ms(status.observed_at, Utc::now());
        let fresh = age_ms <= max_service_status_staleness_ms;
        service_status_age_ms = Some(age_ms);
        service_status_fresh = Some(fresh);
        if !fresh {
            mismatches.push(format!(
                "live activation status artifact {} is stale: observed_at={}, age_ms={}, max_allowed_ms={}",
                runtime_paths.apply_status_path.display(),
                status.observed_at.to_rfc3339(),
                age_ms,
                max_service_status_staleness_ms
            ));
        }
        mismatches.extend(
            tiny_live_activation_live_execute::verify_live_service_status_for_watch(
                &status,
                "activation",
                target_config_path,
                target_service_name,
                &activation_fingerprint.sha256,
                true,
            ),
        );
    } else {
        mismatches.push(format!(
            "live activation status artifact {} is missing",
            runtime_paths.apply_status_path.display()
        ));
    }
    Ok(LiveWatchVerificationSummary {
        service_status_artifact_present,
        backup_proof_present,
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256),
        current_target_execution_enabled: Some(current_target.execution.enabled),
        service_status_observed_at,
        service_status_fresh,
        service_status_age_ms,
        max_service_status_staleness_ms,
        mismatches,
    })
}

fn observed_age_ms(observed_at: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    let age_ms = now.signed_duration_since(observed_at).num_milliseconds();
    if age_ms <= 0 {
        0
    } else {
        age_ms as u64
    }
}

fn evaluate_observation_once(
    config: &Config,
    context: &WatchContext,
    watch_target_kind: WatchTargetKind,
    strict_missing: bool,
) -> Result<ObservationEvaluationSummary> {
    let path = observation_path(&config.runtime_dir);
    if !path.exists() {
        return Ok(ObservationEvaluationSummary {
            observation_path: path.display().to_string(),
            observation_present: false,
            observation_fresh: None,
            target_running: None,
            sample_window_seconds: None,
            window_complete: false,
            total_execution_attempts: None,
            execution_error_rate_pct: None,
            adapter_contract_failure_rate_pct: None,
            policy_echo_mismatch_rate_pct: None,
            fee_or_slippage_breach_rate_pct: None,
            connectivity_degraded_seconds: None,
            daily_realized_loss_sol: None,
            consecutive_hard_failures: None,
            bounded_but_degraded: true,
            keep_running: !strict_missing,
            rollback_triggered: strict_missing,
            warnings: if strict_missing {
                Vec::new()
            } else {
                vec!["watch observation artifact is not present yet; bounded watch loop is still waiting for evidence".to_string()]
            },
            mismatches: if strict_missing {
                vec![format!(
                    "watch observation artifact {} is missing",
                    path.display()
                )]
            } else {
                Vec::new()
            },
            rollback_triggers: if strict_missing {
                vec![format!(
                    "watch observation artifact {} is missing",
                    path.display()
                )]
            } else {
                Vec::new()
            },
        });
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed reading watch observation {}", path.display()))?;
    let observation: WatchObservation = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing watch observation {}", path.display()))?;
    Ok(evaluate_observation(
        &path,
        &observation,
        config,
        context,
        watch_target_kind,
    ))
}

fn evaluate_observation(
    path: &Path,
    observation: &WatchObservation,
    config: &Config,
    context: &WatchContext,
    watch_target_kind: WatchTargetKind,
) -> ObservationEvaluationSummary {
    let mut mismatches = Vec::new();
    let mut rollback_triggers = Vec::new();
    let mut warnings = Vec::new();
    if observation.observation_version != OBSERVATION_VERSION {
        mismatches.push(format!(
            "watch observation version must be {}, found {}",
            OBSERVATION_VERSION, observation.observation_version
        ));
    }
    if observation.watch_target_kind != watch_target_kind.as_str() {
        mismatches.push(format!(
            "watch observation target kind must be {}, found {}",
            watch_target_kind.as_str(),
            observation.watch_target_kind
        ));
    }
    if observation.runtime_dir != config.runtime_dir.display().to_string() {
        mismatches.push(
            "watch observation runtime_dir does not match the explicit runtime dir".to_string(),
        );
    }
    if observation.activation_config_path != config.activation_config_path.display().to_string() {
        mismatches.push(
            "watch observation activation_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if observation.rollback_config_path != config.rollback_config_path.display().to_string() {
        mismatches.push(
            "watch observation rollback_config_path does not match the explicit artifact"
                .to_string(),
        );
    }
    if observation.source_config_fingerprint_sha256
        != context
            .pair
            .activation
            .metadata
            .source_config_fingerprint_sha256
    {
        mismatches.push(
            "watch observation source_config_fingerprint_sha256 does not match the activation artifact"
                .to_string(),
        );
    }
    match watch_target_kind {
        WatchTargetKind::TempRun => {
            if observation.target_config_path.is_some() {
                warnings.push(
                    "temp watch observation carries target_config_path, but temp watch uses only the explicit runtime contract"
                        .to_string(),
                );
            }
        }
        WatchTargetKind::LiveTarget => {
            if observation.target_config_path
                != config
                    .target_config_path
                    .as_ref()
                    .map(|path| path.display().to_string())
            {
                mismatches.push(
                    "watch observation target_config_path does not match the explicit live target"
                        .to_string(),
                );
            }
            if observation.target_service_name != config.target_service_name {
                mismatches.push(
                    "watch observation target_service_name does not match the explicit live target"
                        .to_string(),
                );
            }
        }
    }
    if observation.sample_window_started_at > observation.observed_at {
        mismatches.push(
            "watch observation sample_window_started_at must not be later than observed_at"
                .to_string(),
        );
    }
    if observation.execution_error_count > observation.total_execution_attempts {
        mismatches.push(
            "watch observation execution_error_count cannot exceed total_execution_attempts"
                .to_string(),
        );
    }
    if observation.adapter_contract_failure_count > observation.total_execution_attempts {
        mismatches.push(
            "watch observation adapter_contract_failure_count cannot exceed total_execution_attempts"
                .to_string(),
        );
    }
    if observation.policy_echo_mismatch_count > observation.total_execution_attempts {
        mismatches.push(
            "watch observation policy_echo_mismatch_count cannot exceed total_execution_attempts"
                .to_string(),
        );
    }
    if observation.fee_or_slippage_breach_count > observation.total_execution_attempts {
        mismatches.push(
            "watch observation fee_or_slippage_breach_count cannot exceed total_execution_attempts"
                .to_string(),
        );
    }
    if !observation.daily_realized_loss_sol.is_finite() || observation.daily_realized_loss_sol < 0.0
    {
        mismatches
            .push("watch observation daily_realized_loss_sol must be finite and >= 0".to_string());
    }
    let observed_staleness_ms = (Utc::now() - observation.observed_at)
        .num_milliseconds()
        .max(0) as u64;
    let observation_fresh = observed_staleness_ms <= context.max_observation_staleness_ms;
    let sample_window_seconds = (observation.observed_at - observation.sample_window_started_at)
        .num_seconds()
        .max(0) as u64;
    let window_complete = sample_window_seconds >= context.watch_window_seconds;
    let execution_error_rate_pct = rate_pct(
        observation.execution_error_count,
        observation.total_execution_attempts,
    );
    let adapter_contract_failure_rate_pct = rate_pct(
        observation.adapter_contract_failure_count,
        observation.total_execution_attempts,
    );
    let policy_echo_mismatch_rate_pct = rate_pct(
        observation.policy_echo_mismatch_count,
        observation.total_execution_attempts,
    );
    let fee_or_slippage_breach_rate_pct = rate_pct(
        observation.fee_or_slippage_breach_count,
        observation.total_execution_attempts,
    );
    if !observation_fresh {
        rollback_triggers.push(format!(
            "watch observation is stale by {} ms; max allowed is {} ms",
            observed_staleness_ms, context.max_observation_staleness_ms
        ));
    }
    if !observation.target_running {
        rollback_triggers.push(
            "watch observation indicates the target process/service is not running".to_string(),
        );
    }
    if execution_error_rate_pct > context.guardrail.max_execution_error_rate_pct {
        rollback_triggers.push(format!(
            "execution error rate {:.4}% breached max {:.4}%",
            execution_error_rate_pct, context.guardrail.max_execution_error_rate_pct
        ));
    }
    if adapter_contract_failure_rate_pct > context.guardrail.max_adapter_contract_failure_rate_pct {
        rollback_triggers.push(format!(
            "adapter contract failure rate {:.4}% breached max {:.4}%",
            adapter_contract_failure_rate_pct,
            context.guardrail.max_adapter_contract_failure_rate_pct
        ));
    }
    if policy_echo_mismatch_rate_pct > context.guardrail.max_policy_echo_mismatch_rate_pct {
        rollback_triggers.push(format!(
            "policy echo mismatch rate {:.4}% breached max {:.4}%",
            policy_echo_mismatch_rate_pct, context.guardrail.max_policy_echo_mismatch_rate_pct
        ));
    }
    if fee_or_slippage_breach_rate_pct > context.guardrail.max_fee_or_slippage_breach_rate_pct {
        rollback_triggers.push(format!(
            "fee/slippage breach rate {:.4}% breached max {:.4}%",
            fee_or_slippage_breach_rate_pct, context.guardrail.max_fee_or_slippage_breach_rate_pct
        ));
    }
    if observation.connectivity_degraded_seconds
        > context.guardrail.max_connectivity_degraded_window_seconds
    {
        rollback_triggers.push(format!(
            "connectivity degraded window {}s breached max {}s",
            observation.connectivity_degraded_seconds,
            context.guardrail.max_connectivity_degraded_window_seconds
        ));
    }
    if observation.daily_realized_loss_sol > context.guardrail.max_daily_realized_loss_sol {
        rollback_triggers.push(format!(
            "daily realized loss {:.6} SOL breached max {:.6} SOL",
            observation.daily_realized_loss_sol, context.guardrail.max_daily_realized_loss_sol
        ));
    }
    if observation.consecutive_hard_failures >= context.guardrail.max_consecutive_hard_failures {
        rollback_triggers.push(format!(
            "consecutive hard failures {} reached rollback threshold {}",
            observation.consecutive_hard_failures, context.guardrail.max_consecutive_hard_failures
        ));
    }
    let bounded_but_degraded = rollback_triggers.is_empty()
        && (!window_complete
            || observation.connectivity_degraded_seconds > 0
            || observation.daily_realized_loss_sol > 0.0
            || observation.consecutive_hard_failures > 0
            || observation.execution_error_count > 0
            || observation.adapter_contract_failure_count > 0
            || observation.policy_echo_mismatch_count > 0
            || observation.fee_or_slippage_breach_count > 0
            || observation.total_execution_attempts == 0);
    if bounded_but_degraded {
        warnings.push(
            "watch evidence remains bounded but degraded/incomplete; keep-running stays provisional until a fuller window passes"
                .to_string(),
        );
    }
    if !window_complete && rollback_triggers.is_empty() {
        warnings.push(format!(
            "watch sample window is {}s, below the configured watch window {}s",
            sample_window_seconds, context.watch_window_seconds
        ));
    }
    if !mismatches.is_empty() {
        rollback_triggers.extend(mismatches.clone());
    }
    ObservationEvaluationSummary {
        observation_path: path.display().to_string(),
        observation_present: true,
        observation_fresh: Some(observation_fresh),
        target_running: Some(observation.target_running),
        sample_window_seconds: Some(sample_window_seconds),
        window_complete,
        total_execution_attempts: Some(observation.total_execution_attempts),
        execution_error_rate_pct: Some(execution_error_rate_pct),
        adapter_contract_failure_rate_pct: Some(adapter_contract_failure_rate_pct),
        policy_echo_mismatch_rate_pct: Some(policy_echo_mismatch_rate_pct),
        fee_or_slippage_breach_rate_pct: Some(fee_or_slippage_breach_rate_pct),
        connectivity_degraded_seconds: Some(observation.connectivity_degraded_seconds),
        daily_realized_loss_sol: Some(observation.daily_realized_loss_sol),
        consecutive_hard_failures: Some(observation.consecutive_hard_failures),
        bounded_but_degraded,
        keep_running: rollback_triggers.is_empty(),
        rollback_triggered: !rollback_triggers.is_empty(),
        warnings,
        mismatches,
        rollback_triggers,
    }
}

fn observation_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("tiny_live_activation.watch.observation.json")
}

fn default_missing_observation_summary(
    config: &Config,
    _context: &WatchContext,
) -> ObservationEvaluationSummary {
    ObservationEvaluationSummary {
        observation_path: observation_path(&config.runtime_dir).display().to_string(),
        observation_present: false,
        observation_fresh: None,
        target_running: None,
        sample_window_seconds: None,
        window_complete: false,
        total_execution_attempts: None,
        execution_error_rate_pct: None,
        adapter_contract_failure_rate_pct: None,
        policy_echo_mismatch_rate_pct: None,
        fee_or_slippage_breach_rate_pct: None,
        connectivity_degraded_seconds: None,
        daily_realized_loss_sol: None,
        consecutive_hard_failures: None,
        bounded_but_degraded: true,
        keep_running: false,
        rollback_triggered: true,
        warnings: Vec::new(),
        mismatches: vec![format!(
            "watch observation artifact {} is missing",
            observation_path(&config.runtime_dir).display()
        )],
        rollback_triggers: vec![format!(
            "watch observation artifact {} is missing",
            observation_path(&config.runtime_dir).display()
        )],
    }
}

fn default_temp_verification_failure(
    reason: String,
) -> tiny_live_activation_apply::TempRunVerificationSummary {
    tiny_live_activation_apply::TempRunVerificationSummary {
        pid_file_present: false,
        log_file_present: false,
        status_file_present: false,
        rollback_status_present: false,
        process_running: false,
        heartbeat_fresh: false,
        status_state: None,
        pid: None,
        mismatches: vec![reason],
    }
}

fn watch_decision_report(
    config: &Config,
    context: &WatchContext,
    watch_target_kind: WatchTargetKind,
    verdict: TinyLiveWatchVerdict,
    reason: String,
    bounded_but_degraded: bool,
    observation_evaluation: Option<ObservationEvaluationSummary>,
    temp_runtime_verification: Option<tiny_live_activation_apply::TempRunVerificationSummary>,
    live_watch_verification: Option<LiveWatchVerificationSummary>,
) -> WatchReport {
    WatchReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(context.pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(context.pair.rollback.metadata_path.display().to_string()),
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
        watch_window_seconds: context.watch_window_seconds,
        sample_cadence_ms: context.sample_cadence_ms,
        max_observation_staleness_ms: context.max_observation_staleness_ms,
        watch_target_kind: Some(watch_target_kind.as_str().to_string()),
        decision: Some(match verdict {
            TinyLiveWatchVerdict::TinyLiveWatchTempContinue
            | TinyLiveWatchVerdict::TinyLiveWatchLiveContinue => "keep_running".to_string(),
            TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered
            | TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered
            | TinyLiveWatchVerdict::TinyLiveWatchVerifyInvalid => "rollback_now".to_string(),
            _ => "not_applicable".to_string(),
        }),
        bounded_but_degraded,
        pre_activation_gate_verdict_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_verdict
                .clone(),
        ),
        pre_activation_gate_reason_used: Some(
            context
                .pair
                .activation
                .metadata
                .pre_activation_gate_reason
                .clone(),
        ),
        activation_plan_verdict_used: Some(
            context.pair.activation.metadata.activation_plan_verdict.clone(),
        ),
        activation_plan_reason_used: Some(
            context.pair.activation.metadata.activation_plan_reason.clone(),
        ),
        guardrail_verdict_used: Some(serialize_enum(&context.guardrail.verdict)),
        guardrail_reason_used: Some(context.guardrail.reason.clone()),
        expected_rollback_triggers: expected_rollback_triggers(&context.guardrail),
        verify_watch_command_summary: Some(context.verify_watch_command_summary.clone()),
        watch_temp_command_summary: Some(context.watch_temp_command_summary.clone()),
        watch_live_command_summary: context.watch_live_command_summary.clone(),
        temp_runtime_verification,
        live_watch_verification,
        observation_evaluation,
        activation_authorized: false,
        explicit_statement:
            "watch verdicts only classify bounded continue-versus-rollback posture; they do not authorize or perform production activation by themselves"
                .to_string(),
    }
}

fn invalid_report(
    config: &Config,
    mode: &str,
    reason: String,
    context: Option<&WatchContext>,
    temp_runtime_verification: Option<tiny_live_activation_apply::TempRunVerificationSummary>,
    live_watch_verification: Option<LiveWatchVerificationSummary>,
) -> WatchReport {
    WatchReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: TinyLiveWatchVerdict::TinyLiveWatchVerifyInvalid,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: context
            .map(|value| value.pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: context
            .map(|value| value.pair.rollback.metadata_path.display().to_string()),
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
        watch_window_seconds: context
            .map(|value| value.watch_window_seconds)
            .unwrap_or(config.watch_window_seconds.unwrap_or(0)),
        sample_cadence_ms: config.sample_cadence_ms,
        max_observation_staleness_ms: context
            .map(|value| value.max_observation_staleness_ms)
            .unwrap_or_else(|| {
                config.max_observation_staleness_ms.unwrap_or(std::cmp::max(
                    MIN_OBSERVATION_STALENESS_MS,
                    config.sample_cadence_ms.saturating_mul(OBSERVATION_STALENESS_MULTIPLIER),
                ))
            }),
        watch_target_kind: None,
        decision: None,
        bounded_but_degraded: false,
        pre_activation_gate_verdict_used: context.map(|value| {
            value
                .pair
                .activation
                .metadata
                .pre_activation_gate_verdict
                .clone()
        }),
        pre_activation_gate_reason_used: context.map(|value| {
            value
                .pair
                .activation
                .metadata
                .pre_activation_gate_reason
                .clone()
        }),
        activation_plan_verdict_used: context.map(|value| {
            value
                .pair
                .activation
                .metadata
                .activation_plan_verdict
                .clone()
        }),
        activation_plan_reason_used: context.map(|value| {
            value
                .pair
                .activation
                .metadata
                .activation_plan_reason
                .clone()
        }),
        guardrail_verdict_used: context.map(|value| serialize_enum(&value.guardrail.verdict)),
        guardrail_reason_used: context.map(|value| value.guardrail.reason.clone()),
        expected_rollback_triggers: context
            .map(|value| expected_rollback_triggers(&value.guardrail))
            .unwrap_or_default(),
        verify_watch_command_summary: context.map(|value| value.verify_watch_command_summary.clone()),
        watch_temp_command_summary: context.map(|value| value.watch_temp_command_summary.clone()),
        watch_live_command_summary: context.and_then(|value| value.watch_live_command_summary.clone()),
        temp_runtime_verification,
        live_watch_verification,
        observation_evaluation: None,
        activation_authorized: false,
        explicit_statement:
            "watch verification failed before any continue-versus-rollback decision; this still does not authorize production activation"
                .to_string(),
    }
}

fn build_command_summaries(
    config: &Config,
    watch_window_seconds: u64,
    max_observation_staleness_ms: u64,
) -> Result<(String, String, Option<String>)> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for tiny-live watch")?;
    let base = command_base(
        &exe,
        config,
        watch_window_seconds,
        max_observation_staleness_ms,
    );
    let verify = format!("{base} --verify-watch-target");
    let temp = format!("{base} --watch-temp-run");
    let live = if config.target_config_path.is_some()
        && config.target_service_name.is_some()
        && config.service_control_command_path.is_some()
        && config.backup_dir.is_some()
    {
        Some(format!("{base} --watch-live-target"))
    } else {
        None
    };
    Ok((verify, temp, live))
}

fn command_base_for_script(config: &Config, plan: &WatchReport) -> Result<String> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for tiny-live watch script")?;
    Ok(command_base(
        &exe,
        config,
        plan.watch_window_seconds,
        plan.max_observation_staleness_ms,
    ))
}

fn command_base(
    exe: &Path,
    config: &Config,
    watch_window_seconds: u64,
    max_observation_staleness_ms: u64,
) -> String {
    let mut parts = vec![
        exe.display().to_string(),
        "--activation-config".to_string(),
        config.activation_config_path.display().to_string(),
        "--rollback-config".to_string(),
        config.rollback_config_path.display().to_string(),
        "--runtime-dir".to_string(),
        config.runtime_dir.display().to_string(),
        "--watch-window-seconds".to_string(),
        watch_window_seconds.to_string(),
        "--sample-cadence-ms".to_string(),
        config.sample_cadence_ms.to_string(),
        "--max-observation-staleness-ms".to_string(),
        max_observation_staleness_ms.to_string(),
    ];
    if let Some(path) = &config.target_config_path {
        parts.push("--target-config".to_string());
        parts.push(path.display().to_string());
    }
    if let Some(value) = &config.target_service_name {
        parts.push("--target-service".to_string());
        parts.push(value.clone());
    }
    if let Some(path) = &config.service_control_command_path {
        parts.push("--service-control-command".to_string());
        parts.push(path.display().to_string());
    }
    if let Some(path) = &config.backup_dir {
        parts.push("--backup-dir".to_string());
        parts.push(path.display().to_string());
    }
    parts.join(" ")
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

fn rate_pct(count: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (count as f64 * 100.0) / total as f64
    }
}

fn expected_rollback_triggers(
    report: &tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
) -> Vec<String> {
    report
        .rollback_triggers
        .iter()
        .map(|trigger| match trigger.threshold_kind.as_str() {
            "rate_pct" => format!(
                "{} <= {:.4}% over {:?}s",
                trigger.trigger,
                trigger.threshold_rate_pct.unwrap_or_default(),
                trigger.evaluation_window_seconds.unwrap_or_default()
            ),
            "window_seconds" => format!(
                "{} <= {}s",
                trigger.trigger,
                trigger.threshold_seconds.unwrap_or_default()
            ),
            "absolute_sol" => format!(
                "{} <= {:.6} SOL",
                trigger.trigger,
                trigger.threshold_sol.unwrap_or_default()
            ),
            "count" => format!(
                "{} < {}",
                trigger.trigger,
                trigger.threshold_count.unwrap_or_default()
            ),
            _ => trigger.trigger.clone(),
        })
        .collect()
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanWatch => "plan_watch",
        Mode::RenderWatchScript => "render_watch_script",
        Mode::VerifyWatchTarget => "verify_watch_target",
        Mode::WatchTempRun => "watch_temp_run",
        Mode::WatchLiveTarget => "watch_live_target",
    }
}

fn render_human(report: &WatchReport) -> String {
    let mut lines = vec![
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("mode={}", report.mode),
        format!("reason={}", report.reason),
        format!("activation_config_path={}", report.activation_config_path),
        format!("rollback_config_path={}", report.rollback_config_path),
        format!("runtime_dir={}", report.runtime_dir),
        format!("watch_window_seconds={}", report.watch_window_seconds),
        format!("sample_cadence_ms={}", report.sample_cadence_ms),
        format!(
            "max_observation_staleness_ms={}",
            report.max_observation_staleness_ms
        ),
        format!("bounded_but_degraded={}", report.bounded_but_degraded),
    ];
    if let Some(path) = &report.activation_metadata_path {
        lines.push(format!("activation_metadata_path={path}"));
    }
    if let Some(path) = &report.rollback_metadata_path {
        lines.push(format!("rollback_metadata_path={path}"));
    }
    if let Some(path) = &report.target_config_path {
        lines.push(format!("target_config_path={path}"));
    }
    if let Some(value) = &report.target_service_name {
        lines.push(format!("target_service_name={value}"));
    }
    if let Some(path) = &report.service_control_command_path {
        lines.push(format!("service_control_command_path={path}"));
    }
    if let Some(path) = &report.backup_dir {
        lines.push(format!("backup_dir={path}"));
    }
    if let Some(value) = &report.watch_target_kind {
        lines.push(format!("watch_target_kind={value}"));
    }
    if let Some(value) = &report.decision {
        lines.push(format!("decision={value}"));
    }
    if let Some(value) = &report.pre_activation_gate_verdict_used {
        lines.push(format!("pre_activation_gate_verdict_used={value}"));
    }
    if let Some(value) = &report.pre_activation_gate_reason_used {
        lines.push(format!("pre_activation_gate_reason_used={value}"));
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
    if !report.expected_rollback_triggers.is_empty() {
        lines.push("expected_rollback_triggers:".to_string());
        lines.extend(
            report
                .expected_rollback_triggers
                .iter()
                .map(|entry| format!("  {entry}")),
        );
    }
    if let Some(value) = &report.verify_watch_command_summary {
        lines.push(format!("verify_watch_command_summary={value}"));
    }
    if let Some(value) = &report.watch_temp_command_summary {
        lines.push(format!("watch_temp_command_summary={value}"));
    }
    if let Some(value) = &report.watch_live_command_summary {
        lines.push(format!("watch_live_command_summary={value}"));
    }
    if let Some(verification) = &report.temp_runtime_verification {
        lines.push(format!(
            "temp_runtime_status_artifact_present={}",
            verification.status_file_present
        ));
        lines.push(format!(
            "temp_runtime_process_running={}",
            verification.process_running
        ));
        lines.push(format!(
            "temp_runtime_heartbeat_fresh={}",
            verification.heartbeat_fresh
        ));
        if !verification.mismatches.is_empty() {
            lines.push("temp_runtime_mismatches:".to_string());
            lines.extend(
                verification
                    .mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
    }
    if let Some(verification) = &report.live_watch_verification {
        lines.push(format!(
            "live_service_status_artifact_present={}",
            verification.service_status_artifact_present
        ));
        lines.push(format!(
            "live_backup_proof_present={}",
            verification.backup_proof_present
        ));
        if let Some(value) = &verification.current_target_fingerprint_sha256 {
            lines.push(format!("current_target_fingerprint_sha256={value}"));
        }
        if let Some(value) = verification.current_target_execution_enabled {
            lines.push(format!("current_target_execution_enabled={value}"));
        }
        if let Some(value) = verification.service_status_observed_at {
            lines.push(format!("service_status_observed_at={}", value.to_rfc3339()));
        }
        if let Some(value) = verification.service_status_fresh {
            lines.push(format!("service_status_fresh={value}"));
        }
        if let Some(value) = verification.service_status_age_ms {
            lines.push(format!("service_status_age_ms={value}"));
        }
        lines.push(format!(
            "max_service_status_staleness_ms={}",
            verification.max_service_status_staleness_ms
        ));
        if !verification.mismatches.is_empty() {
            lines.push("live_watch_mismatches:".to_string());
            lines.extend(
                verification
                    .mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
    }
    if let Some(observation) = &report.observation_evaluation {
        lines.push(format!("observation_path={}", observation.observation_path));
        lines.push(format!(
            "observation_present={}",
            observation.observation_present
        ));
        if let Some(value) = observation.observation_fresh {
            lines.push(format!("observation_fresh={value}"));
        }
        if let Some(value) = observation.target_running {
            lines.push(format!("observation_target_running={value}"));
        }
        if let Some(value) = observation.sample_window_seconds {
            lines.push(format!("observation_sample_window_seconds={value}"));
        }
        lines.push(format!(
            "observation_window_complete={}",
            observation.window_complete
        ));
        if let Some(value) = observation.total_execution_attempts {
            lines.push(format!("observation_total_execution_attempts={value}"));
        }
        if let Some(value) = observation.execution_error_rate_pct {
            lines.push(format!("observation_execution_error_rate_pct={value:.4}"));
        }
        if let Some(value) = observation.adapter_contract_failure_rate_pct {
            lines.push(format!(
                "observation_adapter_contract_failure_rate_pct={value:.4}"
            ));
        }
        if let Some(value) = observation.policy_echo_mismatch_rate_pct {
            lines.push(format!(
                "observation_policy_echo_mismatch_rate_pct={value:.4}"
            ));
        }
        if let Some(value) = observation.fee_or_slippage_breach_rate_pct {
            lines.push(format!(
                "observation_fee_or_slippage_breach_rate_pct={value:.4}"
            ));
        }
        if let Some(value) = observation.connectivity_degraded_seconds {
            lines.push(format!("observation_connectivity_degraded_seconds={value}"));
        }
        if let Some(value) = observation.daily_realized_loss_sol {
            lines.push(format!("observation_daily_realized_loss_sol={value:.6}"));
        }
        if let Some(value) = observation.consecutive_hard_failures {
            lines.push(format!("observation_consecutive_hard_failures={value}"));
        }
        if !observation.warnings.is_empty() {
            lines.push("observation_warnings:".to_string());
            lines.extend(
                observation
                    .warnings
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
        if !observation.mismatches.is_empty() {
            lines.push("observation_mismatches:".to_string());
            lines.extend(
                observation
                    .mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
        if !observation.rollback_triggers.is_empty() {
            lines.push("observation_rollback_triggers:".to_string());
            lines.extend(
                observation
                    .rollback_triggers
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
    }
    lines.push("activation_authorized=false".to_string());
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
}

#[allow(dead_code)]
pub(crate) fn build_plan_watch_report_for_drill(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: Option<&Path>,
    target_service_name: Option<&str>,
    service_control_command_path: Option<&Path>,
    backup_dir: Option<&Path>,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Result<WatchReport> {
    build_plan_watch_report(&Config {
        mode: Mode::PlanWatch,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: target_config_path.map(Path::to_path_buf),
        target_service_name: target_service_name.map(str::to_string),
        service_control_command_path: service_control_command_path.map(Path::to_path_buf),
        backup_dir: backup_dir.map(Path::to_path_buf),
        output_path: None,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json: false,
    })
}

#[allow(dead_code)]
pub(crate) fn verify_watch_target_report_for_drill(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Result<WatchReport> {
    verify_watch_target_report(&Config {
        mode: Mode::VerifyWatchTarget,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: Some(target_config_path.to_path_buf()),
        target_service_name: Some(target_service_name.to_string()),
        service_control_command_path: Some(service_control_command_path.to_path_buf()),
        backup_dir: Some(backup_dir.to_path_buf()),
        output_path: None,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json: false,
    })
}

#[allow(dead_code)]
pub(crate) fn watch_temp_run_report_for_drill(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Result<WatchReport> {
    watch_temp_run_report(&Config {
        mode: Mode::WatchTempRun,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: None,
        target_service_name: None,
        service_control_command_path: None,
        backup_dir: None,
        output_path: None,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json: false,
    })
}

#[allow(dead_code)]
pub(crate) fn watch_live_target_report_for_drill(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    runtime_dir: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    backup_dir: &Path,
    watch_window_seconds: Option<u64>,
    sample_cadence_ms: u64,
    max_observation_staleness_ms: Option<u64>,
) -> Result<WatchReport> {
    watch_live_target_report(&Config {
        mode: Mode::WatchLiveTarget,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        target_config_path: Some(target_config_path.to_path_buf()),
        target_service_name: Some(target_service_name.to_string()),
        service_control_command_path: Some(service_control_command_path.to_path_buf()),
        backup_dir: Some(backup_dir.to_path_buf()),
        output_path: None,
        watch_window_seconds,
        sample_cadence_ms,
        max_observation_staleness_ms,
        json: false,
    })
}

#[allow(dead_code)]
pub(crate) fn observation_path_for_drill(runtime_dir: &Path) -> PathBuf {
    observation_path(runtime_dir)
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn set_mode(slot: &mut Option<Mode>, mode: Mode, flag: &str) -> Result<()> {
    if slot.replace(mode).is_some() {
        bail!("choose exactly one mode flag, found duplicate around {flag}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid u64 for {flag}: {raw}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn healthy_temp_watch_yields_continue_verdict() {
        let fixture = temp_fixture("tiny_live_watch_temp_green");
        write_temp_runtime_artifacts(&fixture, Utc::now());
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::TempRun,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            None,
            None,
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            true,
            10,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = watch_temp_run_report(&fixture.watch_config()).expect("watch temp");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchTempContinue
        );
        assert_eq!(report.decision.as_deref(), Some("keep_running"));
        assert!(!report.bounded_but_degraded);
    }

    #[test]
    fn missing_temp_status_artifacts_trigger_rollback() {
        let fixture = temp_fixture("tiny_live_watch_temp_missing_status");
        let report = watch_temp_run_report(&Config {
            watch_window_seconds: Some(1),
            sample_cadence_ms: 100,
            max_observation_staleness_ms: Some(1_000),
            ..fixture.watch_config()
        })
        .expect("watch temp");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchTempRollbackTriggered
        );
        assert!(report.reason.contains("failed reading temp apply session"));
    }

    #[test]
    fn fingerprint_drift_after_activation_is_detected_for_live_watch() {
        let fixture = live_fixture("tiny_live_watch_live_drift");
        write_live_runtime_artifacts(&fixture);
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            true,
            10,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        fs::write(
            &fixture.target_config_path,
            sample_live_target_drifted_config_toml(),
        )
        .unwrap();
        let report = watch_live_target_report(&fixture.watch_config()).expect("watch live");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered
        );
        assert!(report.reason.contains("current live target fingerprint"));
    }

    #[test]
    fn rollback_trigger_breach_yields_live_rollback_triggered_verdict() {
        let fixture = live_fixture("tiny_live_watch_live_breach");
        write_live_runtime_artifacts(&fixture);
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            true,
            10,
            2,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = watch_live_target_report(&fixture.watch_config()).expect("watch live");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered
        );
        assert!(report.reason.contains("execution error rate"));
    }

    #[test]
    fn stale_live_status_artifact_rejects_verify_watch_target() {
        let fixture = live_fixture("tiny_live_watch_verify_stale_status");
        write_live_runtime_artifacts_at(&fixture, Utc::now() - ChronoDuration::seconds(10));
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(5),
            Utc::now(),
            true,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = verify_watch_target_report(&Config {
            max_observation_staleness_ms: Some(1_000),
            ..fixture.watch_config()
        })
        .expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchVerifyInvalid
        );
        assert!(report.reason.contains("live activation status artifact"));
        assert!(report.reason.contains("is stale"));
        assert_eq!(
            report
                .live_watch_verification
                .as_ref()
                .and_then(|summary| summary.service_status_fresh),
            Some(false)
        );
    }

    #[test]
    fn stale_live_status_artifact_triggers_watch_live_rollback() {
        let fixture = live_fixture("tiny_live_watch_live_stale_status");
        write_live_runtime_artifacts_at(&fixture, Utc::now() - ChronoDuration::seconds(10));
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            true,
            10,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = watch_live_target_report(&Config {
            max_observation_staleness_ms: Some(1_000),
            ..fixture.watch_config()
        })
        .expect("watch live");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchLiveRollbackTriggered
        );
        assert!(report.reason.contains("live activation status artifact"));
        assert!(report.reason.contains("is stale"));
    }

    #[test]
    fn healthy_live_watch_yields_continue_verdict() {
        let fixture = live_fixture("tiny_live_watch_live_green");
        write_live_runtime_artifacts_at(&fixture, Utc::now());
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(16),
            Utc::now(),
            true,
            10,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = watch_live_target_report(&Config {
            max_observation_staleness_ms: Some(60_000),
            ..fixture.watch_config()
        })
        .expect("watch live");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchLiveContinue
        );
        assert_eq!(report.decision.as_deref(), Some("keep_running"));
        assert_eq!(
            report
                .live_watch_verification
                .as_ref()
                .and_then(|summary| summary.service_status_fresh),
            Some(true)
        );
    }

    #[test]
    fn temp_watch_requires_isolated_runtime_dir() {
        let fixture = temp_fixture("tiny_live_watch_temp_unsafe");
        let relative_runtime = PathBuf::from("relative-runtime");
        let report = watch_temp_run_report(&Config {
            runtime_dir: relative_runtime,
            ..fixture.watch_config()
        })
        .expect("watch temp");
        assert_eq!(
            report.verdict,
            TinyLiveWatchVerdict::TinyLiveWatchVerifyInvalid
        );
        assert!(report
            .reason
            .contains("runtime dir must be an absolute temp path"));
    }

    #[test]
    fn verify_watch_target_accepts_green_contract() {
        let fixture = live_fixture("tiny_live_watch_verify_green");
        write_live_runtime_artifacts(&fixture);
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(5),
            Utc::now(),
            true,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let report = verify_watch_target_report(&fixture.watch_config()).expect("verify");
        assert_eq!(report.verdict, TinyLiveWatchVerdict::TinyLiveWatchVerifyOk);
    }

    #[test]
    fn live_watch_does_not_mutate_target_config_in_tests() {
        let fixture = live_fixture("tiny_live_watch_live_read_only");
        write_live_runtime_artifacts(&fixture);
        write_watch_observation(
            &fixture.runtime_dir,
            WatchTargetKind::LiveTarget,
            &fixture.activation_config_path,
            &fixture.rollback_config_path,
            Some(&fixture.target_config_path),
            Some("solana-copy-bot.service"),
            &fixture.source_fingerprint_sha256,
            Utc::now() - ChronoDuration::minutes(5),
            Utc::now(),
            true,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
        );
        let before = fs::read(&fixture.target_config_path).unwrap();
        let _ = watch_live_target_report(&fixture.watch_config()).expect("watch live");
        let after = fs::read(&fixture.target_config_path).unwrap();
        assert_eq!(before, after);
    }

    struct TempFixture {
        fixture_dir: PathBuf,
        activation_config_path: PathBuf,
        rollback_config_path: PathBuf,
        runtime_dir: PathBuf,
        source_fingerprint_sha256: String,
    }

    struct LiveFixture {
        _fixture_dir: PathBuf,
        activation_config_path: PathBuf,
        rollback_config_path: PathBuf,
        runtime_dir: PathBuf,
        target_config_path: PathBuf,
        backup_dir: PathBuf,
        service_control_command_path: PathBuf,
        source_fingerprint_sha256: String,
        activation_target_fingerprint_sha256: String,
    }

    impl TempFixture {
        fn watch_config(&self) -> Config {
            Config {
                mode: Mode::WatchTempRun,
                activation_config_path: self.activation_config_path.clone(),
                rollback_config_path: self.rollback_config_path.clone(),
                runtime_dir: self.runtime_dir.clone(),
                target_config_path: None,
                target_service_name: None,
                service_control_command_path: None,
                backup_dir: None,
                output_path: None,
                watch_window_seconds: Some(900),
                sample_cadence_ms: 100,
                max_observation_staleness_ms: Some(3_000),
                json: true,
            }
        }
    }

    impl LiveFixture {
        fn watch_config(&self) -> Config {
            Config {
                mode: Mode::WatchLiveTarget,
                activation_config_path: self.activation_config_path.clone(),
                rollback_config_path: self.rollback_config_path.clone(),
                runtime_dir: self.runtime_dir.clone(),
                target_config_path: Some(self.target_config_path.clone()),
                target_service_name: Some("solana-copy-bot.service".to_string()),
                service_control_command_path: Some(self.service_control_command_path.clone()),
                backup_dir: Some(self.backup_dir.clone()),
                output_path: None,
                watch_window_seconds: Some(900),
                sample_cadence_ms: 100,
                max_observation_staleness_ms: Some(3_000),
                json: true,
            }
        }
    }

    fn temp_fixture(label: &str) -> TempFixture {
        let fixture_dir = temp_dir(label);
        let activation_config_path = fixture_dir.join("rendered.activation.toml");
        let rollback_config_path = fixture_dir.join("rendered.rollback.toml");
        let runtime_dir = fixture_dir.join("runtime");
        let source_config_path = fixture_dir.join("live.server.toml");
        let safe_config = sample_safe_config_toml();
        fs::write(&source_config_path, &safe_config).unwrap();
        let source_loaded = load_from_path(&source_config_path).unwrap();
        let source_fingerprint = activation_decision_packet::build_config_fingerprint(
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
            false,
            true,
        );
        write_rendered_artifact(
            &rollback_config_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_safe_config_toml(),
            &source_config_path,
            &source_fingerprint.sha256,
            false,
            false,
        );
        TempFixture {
            fixture_dir,
            activation_config_path,
            rollback_config_path,
            runtime_dir,
            source_fingerprint_sha256: source_fingerprint.sha256,
        }
    }

    fn live_fixture(label: &str) -> LiveFixture {
        let fixture_dir = temp_dir(label);
        let activation_config_path = fixture_dir.join("rendered.activation.toml");
        let rollback_config_path = fixture_dir.join("rendered.rollback.toml");
        let runtime_dir = fixture_dir.join("runtime");
        let target_config_path = fixture_dir.join("live.server.toml");
        let backup_dir = fixture_dir.join("backup");
        let service_control_command_path = fixture_dir.join("fake-service-control.sh");
        fs::create_dir_all(&backup_dir).unwrap();
        fs::write(&target_config_path, sample_activation_config_toml()).unwrap();
        write_fake_service_control_command(&service_control_command_path);
        let source_safe_path = fixture_dir.join("source.safe.toml");
        let safe_config = sample_safe_config_toml();
        fs::write(&source_safe_path, &safe_config).unwrap();
        let source_loaded = load_from_path(&source_safe_path).unwrap();
        let source_fingerprint = activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source_loaded,
        )
        .unwrap();
        write_rendered_artifact(
            &activation_config_path,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            &target_config_path,
            &source_fingerprint.sha256,
            false,
            true,
        );
        write_rendered_artifact(
            &rollback_config_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_safe_config_toml(),
            &target_config_path,
            &source_fingerprint.sha256,
            false,
            false,
        );
        write_live_backup_proof(
            &backup_dir,
            &target_config_path,
            "solana-copy-bot.service",
            &source_fingerprint.sha256,
            &source_safe_path,
            &activation_config_path,
            &rollback_config_path,
        );
        let activation_loaded = load_from_path(&activation_config_path).unwrap();
        let activation_target_fingerprint_sha256 =
            activation_decision_packet::build_config_fingerprint(
                tiny_live_activation_execute::FINGERPRINT_SCOPE,
                &activation_loaded,
            )
            .unwrap()
            .sha256;
        LiveFixture {
            _fixture_dir: fixture_dir,
            activation_config_path,
            rollback_config_path,
            runtime_dir,
            target_config_path,
            backup_dir,
            service_control_command_path,
            source_fingerprint_sha256: source_fingerprint.sha256,
            activation_target_fingerprint_sha256,
        }
    }

    fn write_temp_runtime_artifacts(fixture: &TempFixture, now: DateTime<Utc>) {
        fs::create_dir_all(&fixture.runtime_dir).unwrap();
        let paths = tiny_live_activation_apply::runtime_paths(&fixture.runtime_dir);
        let session = tiny_live_activation_apply::TempRunSession {
            session_version: "1".to_string(),
            planned_at: now,
            runtime_dir: fixture.runtime_dir.display().to_string(),
            activation_config_path: fixture.activation_config_path.display().to_string(),
            rollback_config_path: fixture.rollback_config_path.display().to_string(),
            activation_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    &fixture.activation_config_path,
                )
                .display()
                .to_string(),
            rollback_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    &fixture.rollback_config_path,
                )
                .display()
                .to_string(),
            source_config_path: fixture
                .fixture_dir
                .join("live.server.toml")
                .display()
                .to_string(),
            source_config_fingerprint_sha256: fixture.source_fingerprint_sha256.clone(),
            activation_pre_activation_gate_verdict: "pre_activation_gates_green".to_string(),
            activation_pre_activation_gate_reason: "green".to_string(),
            activation_plan_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
            activation_plan_reason: "ready".to_string(),
            startup_timeout_ms: 5_000,
            rollback_timeout_ms: 5_000,
            pid_path: paths.pid_path.display().to_string(),
            log_path: paths.log_path.display().to_string(),
            status_path: paths.status_path.display().to_string(),
            stop_request_path: paths.stop_request_path.display().to_string(),
            rollback_status_path: paths.rollback_status_path.display().to_string(),
            apply_command_summary: "temp apply".to_string(),
            verify_command_summary: "temp verify".to_string(),
            rollback_command_summary: "temp rollback".to_string(),
            explicit_statement: "test".to_string(),
        };
        fs::write(
            &paths.session_path,
            serde_json::to_string_pretty(&session).unwrap(),
        )
        .unwrap();
        fs::write(&paths.pid_path, std::process::id().to_string()).unwrap();
        fs::write(&paths.log_path, "heartbeat\n").unwrap();
        let status = tiny_live_activation_apply::TempRunStatus {
            status_version: "1".to_string(),
            state: "activation_running".to_string(),
            started_at: now - ChronoDuration::minutes(10),
            last_heartbeat_at: now,
            stopped_at: None,
            pid: std::process::id(),
            runtime_dir: fixture.runtime_dir.display().to_string(),
            command_summary: "worker".to_string(),
            activation_config_path: fixture.activation_config_path.display().to_string(),
            rollback_config_path: fixture.rollback_config_path.display().to_string(),
            activation_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    &fixture.activation_config_path,
                )
                .display()
                .to_string(),
            rollback_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    &fixture.rollback_config_path,
                )
                .display()
                .to_string(),
            source_config_fingerprint_sha256: fixture.source_fingerprint_sha256.clone(),
            execution_enabled_in_activation_config: true,
            execution_enabled_in_rollback_config: false,
            activation_authorized: false,
            note: "test".to_string(),
        };
        fs::write(
            &paths.status_path,
            serde_json::to_string_pretty(&status).unwrap(),
        )
        .unwrap();
    }

    fn write_live_runtime_artifacts(fixture: &LiveFixture) {
        write_live_runtime_artifacts_at(fixture, Utc::now());
    }

    fn write_live_runtime_artifacts_at(fixture: &LiveFixture, observed_at: DateTime<Utc>) {
        fs::create_dir_all(&fixture.runtime_dir).unwrap();
        let paths = tiny_live_activation_live_execute::runtime_paths(&fixture.runtime_dir);
        let status = tiny_live_activation_live_execute::LiveServiceStatus {
            status_version: "1".to_string(),
            action: "activation".to_string(),
            observed_at,
            service_name: "solana-copy-bot.service".to_string(),
            target_config_path: fixture.target_config_path.display().to_string(),
            expected_config_fingerprint_sha256: fixture
                .activation_target_fingerprint_sha256
                .clone(),
            observed_config_fingerprint_sha256: fixture
                .activation_target_fingerprint_sha256
                .clone(),
            expected_execution_enabled: true,
            observed_execution_enabled: true,
            restart_successful: true,
            note: "fake service control".to_string(),
        };
        fs::write(
            &paths.apply_status_path,
            serde_json::to_string_pretty(&status).unwrap(),
        )
        .unwrap();
        fs::write(&paths.log_path, "live-started\n").unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn write_watch_observation(
        runtime_dir: &Path,
        watch_target_kind: WatchTargetKind,
        activation_config_path: &Path,
        rollback_config_path: &Path,
        target_config_path: Option<&Path>,
        target_service_name: Option<&str>,
        source_fingerprint_sha256: &str,
        sample_window_started_at: DateTime<Utc>,
        observed_at: DateTime<Utc>,
        target_running: bool,
        total_execution_attempts: u64,
        execution_error_count: u64,
        adapter_contract_failure_count: u64,
        policy_echo_mismatch_count: u64,
        fee_or_slippage_breach_count: u64,
        connectivity_degraded_seconds: u64,
        daily_realized_loss_sol: f64,
        consecutive_hard_failures: u32,
    ) {
        let observation = WatchObservation {
            observation_version: OBSERVATION_VERSION.to_string(),
            watch_target_kind: watch_target_kind.as_str().to_string(),
            observed_at,
            sample_window_started_at,
            runtime_dir: runtime_dir.display().to_string(),
            activation_config_path: activation_config_path.display().to_string(),
            rollback_config_path: rollback_config_path.display().to_string(),
            target_config_path: target_config_path.map(|path| path.display().to_string()),
            target_service_name: target_service_name.map(str::to_string),
            source_config_fingerprint_sha256: source_fingerprint_sha256.to_string(),
            target_running,
            total_execution_attempts,
            execution_error_count,
            adapter_contract_failure_count,
            policy_echo_mismatch_count,
            fee_or_slippage_breach_count,
            connectivity_degraded_seconds,
            daily_realized_loss_sol,
            consecutive_hard_failures,
            note: "test".to_string(),
        };
        fs::create_dir_all(runtime_dir).unwrap();
        fs::write(
            observation_path(runtime_dir),
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
        source_execution_enabled: bool,
        target_execution_enabled: bool,
    ) {
        fs::write(path, &contents).unwrap();
        let hash = sha256_hex(contents.as_bytes());
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
            rendered_config_sha256: hash.clone(),
            pre_activation_gate_verdict: "pre_activation_gates_green".to_string(),
            pre_activation_gate_reason: "green".to_string(),
            activation_plan_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
            activation_plan_reason: "ready".to_string(),
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            field_expectations: vec![tiny_live_activation_execute::FieldExpectation {
                field: "execution.enabled".to_string(),
                source_value: serde_json::json!(source_execution_enabled),
                target_value: serde_json::json!(target_execution_enabled),
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

    fn write_live_backup_proof(
        backup_dir: &Path,
        target_config_path: &Path,
        target_service_name: &str,
        source_fingerprint_sha256: &str,
        source_safe_config_path: &Path,
        activation_config_path: &Path,
        rollback_config_path: &Path,
    ) {
        let (backup_config_path, backup_metadata_path) =
            tiny_live_activation_live_execute::backup_artifact_paths_for_watch(
                target_config_path,
                target_service_name,
                backup_dir,
                source_fingerprint_sha256,
            );
        let backup_bytes = fs::read(source_safe_config_path).unwrap();
        fs::write(&backup_config_path, &backup_bytes).unwrap();
        let metadata = tiny_live_activation_live_execute::LiveBackupMetadata {
            metadata_version: "1".to_string(),
            created_at: ts("2026-03-27T12:00:00Z"),
            target_service_name: target_service_name.to_string(),
            target_config_path: target_config_path.display().to_string(),
            backed_up_config_path: backup_config_path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_execute::FINGERPRINT_SCOPE
                .to_string(),
            source_config_fingerprint_sha256: source_fingerprint_sha256.to_string(),
            backed_up_config_sha256: sha256_hex(&backup_bytes),
            activation_config_path: activation_config_path.display().to_string(),
            activation_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    activation_config_path,
                )
                .display()
                .to_string(),
            rollback_config_path: rollback_config_path.display().to_string(),
            rollback_metadata_path:
                tiny_live_activation_execute::metadata_path_for_rendered_config(
                    rollback_config_path,
                )
                .display()
                .to_string(),
            explicit_statement: "test".to_string(),
        };
        fs::write(
            backup_metadata_path,
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
    }

    fn write_fake_service_control_command(path: &Path) {
        fs::write(path, "#!/usr/bin/env bash\nexit 0\n").unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn sample_safe_config_toml() -> String {
        sample_activation_config_toml().replace("enabled = true", "enabled = false")
    }

    fn sample_live_target_drifted_config_toml() -> String {
        sample_activation_config_toml()
            .replace("copy_notional_sol = 0.05", "copy_notional_sol = 0.04")
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
max_consecutive_hard_failures = 3

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

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}_{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
}
