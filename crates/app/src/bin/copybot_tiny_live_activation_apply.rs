use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_execute.rs"]
pub(crate) mod tiny_live_activation_execute;

const USAGE: &str = "usage: copybot_tiny_live_activation_apply --activation-config <path> --rollback-config <path> --runtime-dir <path> [--json] [--startup-timeout-ms <ms>] [--rollback-timeout-ms <ms>] (--plan | --render-apply-script --output <path> | --render-rollback-script --output <path> | --apply-temp-run | --verify-temp-run | --rollback-temp-run)";
const SESSION_VERSION: &str = "1";
const STATUS_VERSION: &str = "1";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_ROLLBACK_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 250;
const DEFAULT_HEARTBEAT_STALE_MS: u64 = 5_000;

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
    output_path: Option<PathBuf>,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plan,
    RenderApplyScript,
    RenderRollbackScript,
    ApplyTempRun,
    VerifyTempRun,
    RollbackTempRun,
    InternalTempRunWorker,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveActivationApplyVerdict {
    TinyLiveActivationApplyPlanReady,
    TinyLiveActivationApplyScriptRendered,
    TinyLiveActivationApplyRefusedByStage3,
    TinyLiveActivationApplyRefusedByPreActivationGate,
    TinyLiveActivationApplyRefusedByInvalidRenderedArtifact,
    TinyLiveActivationApplyRefusedByUnsafeRuntimeDir,
    TinyLiveActivationTempRunStarted,
    TinyLiveActivationTempRunVerifyOk,
    TinyLiveActivationTempRunVerifyFailed,
    TinyLiveRollbackApplyScriptRendered,
    TinyLiveRollbackTempRunCompleted,
    TinyLiveRollbackTempRunVerifyFailed,
}

#[derive(Debug, Clone)]
struct ValidatedArtifactPair {
    activation: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
    rollback: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimePaths {
    pub(crate) runtime_dir: PathBuf,
    pub(crate) session_path: PathBuf,
    pub(crate) pid_path: PathBuf,
    pub(crate) log_path: PathBuf,
    pub(crate) status_path: PathBuf,
    pub(crate) stop_request_path: PathBuf,
    pub(crate) rollback_status_path: PathBuf,
}

#[derive(Debug, Clone)]
struct WorkerLaunchRequest {
    runtime_dir: PathBuf,
    activation_config_path: PathBuf,
    rollback_config_path: PathBuf,
    log_path: PathBuf,
}

#[derive(Debug)]
struct WorkerLaunch {
    child: Child,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TempRunSession {
    pub(crate) session_version: String,
    pub(crate) planned_at: DateTime<Utc>,
    pub(crate) runtime_dir: String,
    pub(crate) activation_config_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) activation_metadata_path: String,
    pub(crate) rollback_metadata_path: String,
    pub(crate) source_config_path: String,
    pub(crate) source_config_fingerprint_sha256: String,
    pub(crate) activation_pre_activation_gate_verdict: String,
    pub(crate) activation_pre_activation_gate_reason: String,
    pub(crate) activation_plan_verdict: String,
    pub(crate) activation_plan_reason: String,
    pub(crate) startup_timeout_ms: u64,
    pub(crate) rollback_timeout_ms: u64,
    pub(crate) pid_path: String,
    pub(crate) log_path: String,
    pub(crate) status_path: String,
    pub(crate) stop_request_path: String,
    pub(crate) rollback_status_path: String,
    pub(crate) apply_command_summary: String,
    pub(crate) verify_command_summary: String,
    pub(crate) rollback_command_summary: String,
    pub(crate) explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TempRunStatus {
    pub(crate) status_version: String,
    pub(crate) state: String,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) last_heartbeat_at: DateTime<Utc>,
    pub(crate) stopped_at: Option<DateTime<Utc>>,
    pub(crate) pid: u32,
    pub(crate) runtime_dir: String,
    pub(crate) command_summary: String,
    pub(crate) activation_config_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) activation_metadata_path: String,
    pub(crate) rollback_metadata_path: String,
    pub(crate) source_config_fingerprint_sha256: String,
    pub(crate) execution_enabled_in_activation_config: bool,
    pub(crate) execution_enabled_in_rollback_config: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RollbackStatus {
    pub(crate) status_version: String,
    pub(crate) rolled_back_at: DateTime<Utc>,
    pub(crate) runtime_dir: String,
    pub(crate) activation_config_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) source_config_fingerprint_sha256: String,
    pub(crate) activation_process_stopped: bool,
    pub(crate) rollback_execution_enabled: bool,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TempRunVerificationSummary {
    pub(crate) pid_file_present: bool,
    pub(crate) log_file_present: bool,
    pub(crate) status_file_present: bool,
    pub(crate) rollback_status_present: bool,
    pub(crate) process_running: bool,
    pub(crate) heartbeat_fresh: bool,
    pub(crate) status_state: Option<String>,
    pub(crate) pid: Option<u32>,
    pub(crate) mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ApplyReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveActivationApplyVerdict,
    reason: String,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    activation_metadata_path: Option<String>,
    rollback_metadata_path: Option<String>,
    apply_command_summary: Option<String>,
    verify_command_summary: Option<String>,
    rollback_command_summary: Option<String>,
    temp_run_verification: Option<TempRunVerificationSummary>,
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
    let mut activation_config_path: Option<PathBuf> = None;
    let mut rollback_config_path: Option<PathBuf> = None;
    let mut runtime_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut startup_timeout_ms = DEFAULT_STARTUP_TIMEOUT_MS;
    let mut rollback_timeout_ms = DEFAULT_ROLLBACK_TIMEOUT_MS;
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
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--startup-timeout-ms" => {
                startup_timeout_ms = parse_u64_arg("--startup-timeout-ms", args.next())?
            }
            "--rollback-timeout-ms" => {
                rollback_timeout_ms = parse_u64_arg("--rollback-timeout-ms", args.next())?
            }
            "--json" => json = true,
            "--plan" => set_mode(&mut mode, Mode::Plan, "--plan")?,
            "--render-apply-script" => {
                set_mode(&mut mode, Mode::RenderApplyScript, "--render-apply-script")?
            }
            "--render-rollback-script" => set_mode(
                &mut mode,
                Mode::RenderRollbackScript,
                "--render-rollback-script",
            )?,
            "--apply-temp-run" => set_mode(&mut mode, Mode::ApplyTempRun, "--apply-temp-run")?,
            "--verify-temp-run" => set_mode(&mut mode, Mode::VerifyTempRun, "--verify-temp-run")?,
            "--rollback-temp-run" => {
                set_mode(&mut mode, Mode::RollbackTempRun, "--rollback-temp-run")?
            }
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
            "select exactly one mode: --plan | --render-apply-script | --render-rollback-script | --apply-temp-run | --verify-temp-run | --rollback-temp-run"
        )
    })?;
    let activation_config_path =
        activation_config_path.ok_or_else(|| anyhow!("missing required --activation-config"))?;
    let rollback_config_path =
        rollback_config_path.ok_or_else(|| anyhow!("missing required --rollback-config"))?;
    let runtime_dir = runtime_dir.ok_or_else(|| anyhow!("missing required --runtime-dir"))?;

    match mode {
        Mode::RenderApplyScript | Mode::RenderRollbackScript => {
            if output_path.is_none() {
                bail!("--output is required for script render modes");
            }
        }
        _ => {
            if output_path.is_some() {
                bail!("--output is only valid with script render modes");
            }
        }
    }

    Ok(Some(Config {
        mode,
        activation_config_path,
        rollback_config_path,
        runtime_dir,
        output_path,
        startup_timeout_ms: startup_timeout_ms.max(1_000),
        rollback_timeout_ms: rollback_timeout_ms.max(1_000),
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::Plan => build_plan_report(&config),
        Mode::RenderApplyScript => render_script_report(&config, ScriptKind::Apply),
        Mode::RenderRollbackScript => render_script_report(&config, ScriptKind::Rollback),
        Mode::ApplyTempRun => apply_temp_run(&config),
        Mode::VerifyTempRun => verify_temp_run(&config),
        Mode::RollbackTempRun => rollback_temp_run(&config),
        Mode::InternalTempRunWorker => run_internal_temp_run_worker(&config),
    }?;
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

#[derive(Debug, Clone, Copy)]
enum ScriptKind {
    Apply,
    Rollback,
}

fn build_plan_report(config: &Config) -> Result<ApplyReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(pair) => pair,
        Err(error) => return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByInvalidRenderedArtifact,
            format!("{error:#}"),
            None,
            None,
        )),
    };
    if let Err(error) = validate_runtime_dir(&config.runtime_dir) {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir,
            error.to_string(),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.pre_activation_gate_verdict == "blocked_by_stage3" {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByStage3,
            format!(
                "activation artifact remains blocked by Stage 3: {}",
                pair.activation.metadata.pre_activation_gate_reason
            ),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.pre_activation_gate_verdict != "pre_activation_gates_green" {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByPreActivationGate,
            format!(
                "activation artifact pre-activation gate is non-green: {}",
                pair.activation.metadata.pre_activation_gate_reason
            ),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.activation_plan_verdict
        != "activation_plan_ready_when_stage_gate_allows"
    {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByInvalidRenderedArtifact,
            format!(
                "activation artifact does not carry a ready bounded activation-plan verdict: {}",
                pair.activation.metadata.activation_plan_verdict
            ),
            Some(&pair),
            None,
        ));
    }
    let session = planned_session(config, &pair)?;
    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: "plan".to_string(),
        verdict: TinyLiveActivationApplyVerdict::TinyLiveActivationApplyPlanReady,
        reason: "bounded temp apply rehearsal contract is explicit; it still does not authorize or perform production activation".to_string(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        apply_command_summary: Some(session.apply_command_summary),
        verify_command_summary: Some(session.verify_command_summary),
        rollback_command_summary: Some(session.rollback_command_summary),
        temp_run_verification: None,
        explicit_statement:
            "this command rehearses bounded activation/rollback mechanics in an isolated temp runtime only; it does not authorize production activation or touch the real prod service"
                .to_string(),
    })
}

fn render_script_report(config: &Config, kind: ScriptKind) -> Result<ApplyReport> {
    let plan = build_plan_report(config)?;
    if plan.verdict != TinyLiveActivationApplyVerdict::TinyLiveActivationApplyPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("script render modes require --output"))?;
    ensure_new_output_path(output_path)?;
    let command = match kind {
        ScriptKind::Apply => plan
            .apply_command_summary
            .clone()
            .ok_or_else(|| anyhow!("missing apply command summary"))?,
        ScriptKind::Rollback => plan
            .rollback_command_summary
            .clone()
            .ok_or_else(|| anyhow!("missing rollback command summary"))?,
    };
    let script = format!("#!/usr/bin/env bash\nset -euo pipefail\n{}\n", command);
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: match kind {
            ScriptKind::Apply => "render_apply_script".to_string(),
            ScriptKind::Rollback => "render_rollback_script".to_string(),
        },
        verdict: match kind {
            ScriptKind::Apply => TinyLiveActivationApplyVerdict::TinyLiveActivationApplyScriptRendered,
            ScriptKind::Rollback => TinyLiveActivationApplyVerdict::TinyLiveRollbackApplyScriptRendered,
        },
        reason: match kind {
            ScriptKind::Apply => {
                "bounded temp activation rehearsal script rendered".to_string()
            }
            ScriptKind::Rollback => {
                "bounded temp rollback rehearsal script rendered".to_string()
            }
        },
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        activation_metadata_path: plan.activation_metadata_path,
        rollback_metadata_path: plan.rollback_metadata_path,
        apply_command_summary: plan.apply_command_summary,
        verify_command_summary: plan.verify_command_summary,
        rollback_command_summary: plan.rollback_command_summary,
        temp_run_verification: None,
        explicit_statement:
            "rendered rehearsal scripts still target only an isolated temp runtime and do not authorize production activation"
                .to_string(),
    })
}

fn apply_temp_run(config: &Config) -> Result<ApplyReport> {
    apply_temp_run_with_spawner(config, default_spawn_worker)
}

fn verify_temp_run(config: &Config) -> Result<ApplyReport> {
    if let Err(error) = validate_runtime_dir(&config.runtime_dir) {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir,
            error.to_string(),
            None,
            None,
        ));
    }
    let session = load_session(&runtime_paths(&config.runtime_dir).session_path)?;
    let pair = load_validated_artifact_pair_from_session(&session)?;
    let verification = verify_temp_runtime(&runtime_paths(&config.runtime_dir), &session)?;
    let verdict = if verification.mismatches.is_empty() {
        TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyOk
    } else {
        TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyFailed
    };
    let reason = if verification.mismatches.is_empty() {
        "temp activation rehearsal runtime artifacts remain healthy and bounded".to_string()
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "temp activation rehearsal verification failed".to_string())
    };
    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: "verify_temp_run".to_string(),
        verdict,
        reason,
        activation_config_path: session.activation_config_path.clone(),
        rollback_config_path: session.rollback_config_path.clone(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: session.startup_timeout_ms,
        rollback_timeout_ms: session.rollback_timeout_ms,
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        apply_command_summary: Some(session.apply_command_summary.clone()),
        verify_command_summary: Some(session.verify_command_summary.clone()),
        rollback_command_summary: Some(session.rollback_command_summary.clone()),
        temp_run_verification: Some(verification),
        explicit_statement:
            "temp-run verification only confirms isolated rehearsal runtime health; it does not authorize production activation"
                .to_string(),
    })
}

fn rollback_temp_run(config: &Config) -> Result<ApplyReport> {
    if let Err(error) = validate_runtime_dir(&config.runtime_dir) {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir,
            error.to_string(),
            None,
            None,
        ));
    }
    let paths = runtime_paths(&config.runtime_dir);
    let session = load_session(&paths.session_path)?;
    let pair = load_validated_artifact_pair_from_session(&session)?;
    write_text_file(&paths.stop_request_path, "stop\n")?;
    let pid = read_pid(&paths.pid_path)?;
    let deadline = Instant::now() + Duration::from_millis(session.rollback_timeout_ms);
    while process_is_alive(pid) {
        if Instant::now() >= deadline {
            return Ok(ApplyReport {
                generated_at: Utc::now(),
                mode: "rollback_temp_run".to_string(),
                verdict: TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunVerifyFailed,
                reason: format!(
                    "temp activation worker pid {} did not stop within {} ms",
                    pid, session.rollback_timeout_ms
                ),
                activation_config_path: session.activation_config_path.clone(),
                rollback_config_path: session.rollback_config_path.clone(),
                runtime_dir: config.runtime_dir.display().to_string(),
                startup_timeout_ms: session.startup_timeout_ms,
                rollback_timeout_ms: session.rollback_timeout_ms,
                activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
                rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
                apply_command_summary: Some(session.apply_command_summary.clone()),
                verify_command_summary: Some(session.verify_command_summary.clone()),
                rollback_command_summary: Some(session.rollback_command_summary.clone()),
                temp_run_verification: Some(verify_temp_runtime(&paths, &session)?),
                explicit_statement:
                    "rollback rehearsal failed in temp runtime; production activation is still not authorized"
                        .to_string(),
            });
        }
        thread::sleep(Duration::from_millis(100));
    }
    if paths.stop_request_path.exists() {
        fs::remove_file(&paths.stop_request_path).ok();
    }
    let rollback_status = RollbackStatus {
        status_version: STATUS_VERSION.to_string(),
        rolled_back_at: Utc::now(),
        runtime_dir: config.runtime_dir.display().to_string(),
        activation_config_path: session.activation_config_path.clone(),
        rollback_config_path: session.rollback_config_path.clone(),
        source_config_fingerprint_sha256: session.source_config_fingerprint_sha256.clone(),
        activation_process_stopped: true,
        rollback_execution_enabled: pair.rollback.rendered_config.execution.enabled,
        note:
            "temp rollback rehearsal stopped the isolated activation worker and re-validated the paired rollback artifact with execution.enabled=false"
                .to_string(),
    };
    write_text_file(
        &paths.rollback_status_path,
        &serde_json::to_string_pretty(&rollback_status)?,
    )?;
    let verification = verify_rollback_runtime(&paths, &session, &rollback_status)?;
    let verdict = if verification.mismatches.is_empty() {
        TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunCompleted
    } else {
        TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunVerifyFailed
    };
    let reason = if verification.mismatches.is_empty() {
        "temp rollback rehearsal completed and left explicit disabled-posture evidence under the runtime dir".to_string()
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "temp rollback rehearsal verification failed".to_string())
    };
    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: "rollback_temp_run".to_string(),
        verdict,
        reason,
        activation_config_path: session.activation_config_path.clone(),
        rollback_config_path: session.rollback_config_path.clone(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: session.startup_timeout_ms,
        rollback_timeout_ms: session.rollback_timeout_ms,
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        apply_command_summary: Some(session.apply_command_summary.clone()),
        verify_command_summary: Some(session.verify_command_summary.clone()),
        rollback_command_summary: Some(session.rollback_command_summary.clone()),
        temp_run_verification: Some(verification),
        explicit_statement:
            "rollback rehearsal only proves isolated temp runtime recovery mechanics; it does not authorize production activation"
                .to_string(),
    })
}

fn run_internal_temp_run_worker(config: &Config) -> Result<ApplyReport> {
    validate_runtime_dir(&config.runtime_dir)?;
    let pair = load_validated_artifact_pair(config)?;
    let paths = runtime_paths(&config.runtime_dir);
    let pid = std::process::id();
    let command_summary = default_worker_command_summary(config)?;
    loop {
        let now = Utc::now();
        let status = TempRunStatus {
            status_version: STATUS_VERSION.to_string(),
            state: if paths.stop_request_path.exists() {
                "activation_stopped".to_string()
            } else {
                "activation_running".to_string()
            },
            started_at: now,
            last_heartbeat_at: now,
            stopped_at: if paths.stop_request_path.exists() {
                Some(now)
            } else {
                None
            },
            pid,
            runtime_dir: config.runtime_dir.display().to_string(),
            command_summary: command_summary.clone(),
            activation_config_path: config.activation_config_path.display().to_string(),
            rollback_config_path: config.rollback_config_path.display().to_string(),
            activation_metadata_path: pair.activation.metadata_path.display().to_string(),
            rollback_metadata_path: pair.rollback.metadata_path.display().to_string(),
            source_config_fingerprint_sha256: pair
                .activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
            execution_enabled_in_activation_config: pair.activation.rendered_config.execution.enabled,
            execution_enabled_in_rollback_config: pair.rollback.rendered_config.execution.enabled,
            activation_authorized: false,
            note:
                "isolated temp rehearsal worker only proves process/runtime mechanics over rendered artifacts; it does not start the real prod service"
                    .to_string(),
        };
        write_text_file(&paths.status_path, &serde_json::to_string_pretty(&status)?)?;
        if paths.stop_request_path.exists() {
            break;
        }
        println!(
            "tiny-live temp worker heartbeat pid={} activation_config={}",
            pid,
            config.activation_config_path.display()
        );
        thread::sleep(Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS));
    }

    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: "internal_temp_run_worker".to_string(),
        verdict: TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted,
        reason: "internal temp worker exited after stop request".to_string(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        apply_command_summary: Some(command_summary.clone()),
        verify_command_summary: None,
        rollback_command_summary: None,
        temp_run_verification: None,
        explicit_statement:
            "internal temp worker exited cleanly; production activation remains unauthorized"
                .to_string(),
    })
}

fn apply_temp_run_with_spawner<F>(config: &Config, spawner: F) -> Result<ApplyReport>
where
    F: Fn(&WorkerLaunchRequest) -> Result<WorkerLaunch>,
{
    let pair = match load_validated_artifact_pair(config) {
        Ok(pair) => pair,
        Err(error) => return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByInvalidRenderedArtifact,
            format!("{error:#}"),
            None,
            None,
        )),
    };
    if let Err(error) = validate_runtime_dir(&config.runtime_dir) {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir,
            error.to_string(),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.pre_activation_gate_verdict == "blocked_by_stage3" {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByStage3,
            format!(
                "activation artifact remains blocked by Stage 3: {}",
                pair.activation.metadata.pre_activation_gate_reason
            ),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.pre_activation_gate_verdict != "pre_activation_gates_green" {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByPreActivationGate,
            format!(
                "activation artifact pre-activation gate is non-green: {}",
                pair.activation.metadata.pre_activation_gate_reason
            ),
            Some(&pair),
            None,
        ));
    }
    if pair.activation.metadata.activation_plan_verdict
        != "activation_plan_ready_when_stage_gate_allows"
    {
        return Ok(refusal_report(
            config,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByInvalidRenderedArtifact,
            "activation artifact does not carry a ready activation-plan verdict".to_string(),
            Some(&pair),
            None,
        ));
    }

    let paths = runtime_paths(&config.runtime_dir);
    fs::create_dir_all(&paths.runtime_dir).with_context(|| {
        format!(
            "failed creating temp runtime dir {}",
            paths.runtime_dir.display()
        )
    })?;
    if paths.stop_request_path.exists() {
        fs::remove_file(&paths.stop_request_path).ok();
    }
    if paths.pid_path.exists() {
        let stale_pid = read_pid(&paths.pid_path).ok();
        if stale_pid.is_some_and(process_is_alive) {
            return Ok(refusal_report(
                config,
                TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyFailed,
                format!(
                    "temp runtime already has a live activation worker pid {}",
                    stale_pid.unwrap_or_default()
                ),
                Some(&pair),
                None,
            ));
        }
    }

    let launch = spawner(&WorkerLaunchRequest {
        runtime_dir: config.runtime_dir.clone(),
        activation_config_path: config.activation_config_path.clone(),
        rollback_config_path: config.rollback_config_path.clone(),
        log_path: paths.log_path.clone(),
    })?;
    write_text_file(&paths.pid_path, &launch.child.id().to_string())?;
    let session = planned_session(config, &pair)?;
    write_text_file(
        &paths.session_path,
        &serde_json::to_string_pretty(&session)?,
    )?;
    let verification = wait_for_started_runtime(&paths, &session, config.startup_timeout_ms)?;
    let verdict = if verification.mismatches.is_empty() {
        TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
    } else {
        TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyFailed
    };
    let reason = if verification.mismatches.is_empty() {
        "isolated temp activation worker started and wrote bounded runtime artifacts under the explicit runtime dir".to_string()
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "temp activation rehearsal failed to start cleanly".to_string())
    };
    Ok(ApplyReport {
        generated_at: Utc::now(),
        mode: "apply_temp_run".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        apply_command_summary: Some(session.apply_command_summary.clone()),
        verify_command_summary: Some(session.verify_command_summary.clone()),
        rollback_command_summary: Some(session.rollback_command_summary.clone()),
        temp_run_verification: Some(verification),
        explicit_statement:
            "temp apply rehearsal launches only an isolated helper worker inside the explicit runtime dir; it does not authorize or perform real production activation"
                .to_string(),
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
    validate_pair_consistency(&activation, &rollback)?;
    Ok(ValidatedArtifactPair {
        activation,
        rollback,
    })
}

fn load_validated_artifact_pair_from_session(
    session: &TempRunSession,
) -> Result<ValidatedArtifactPair> {
    let activation_path = PathBuf::from(&session.activation_config_path);
    let rollback_path = PathBuf::from(&session.rollback_config_path);
    let activation =
        tiny_live_activation_execute::inspect_rendered_config_artifact(&activation_path)
            .with_context(|| {
                format!(
                    "activation rendered artifact {} is invalid",
                    activation_path.display()
                )
            })?;
    let rollback = tiny_live_activation_execute::inspect_rendered_config_artifact(&rollback_path)
        .with_context(|| {
        format!(
            "rollback rendered artifact {} is invalid",
            rollback_path.display()
        )
    })?;
    validate_pair_consistency(&activation, &rollback)?;
    Ok(ValidatedArtifactPair {
        activation,
        rollback,
    })
}

fn validate_pair_consistency(
    activation: &tiny_live_activation_execute::LoadedRenderedConfigArtifact,
    rollback: &tiny_live_activation_execute::LoadedRenderedConfigArtifact,
) -> Result<()> {
    if activation.metadata.render_kind != tiny_live_activation_execute::RenderKind::Activation {
        bail!("activation artifact must have render_kind=activation");
    }
    if rollback.metadata.render_kind != tiny_live_activation_execute::RenderKind::Rollback {
        bail!("rollback artifact must have render_kind=rollback");
    }
    if !activation.rendered_config.execution.enabled {
        bail!("activation artifact must keep execution.enabled=true");
    }
    if rollback.rendered_config.execution.enabled {
        bail!("rollback artifact must keep execution.enabled=false");
    }
    if activation.metadata.source_config_fingerprint_sha256
        != rollback.metadata.source_config_fingerprint_sha256
    {
        bail!("activation and rollback artifacts must share the same source config fingerprint");
    }
    if activation.metadata.source_config_fingerprint_scope
        != rollback.metadata.source_config_fingerprint_scope
    {
        bail!(
            "activation and rollback artifacts must share the same source config fingerprint scope"
        );
    }
    if activation.metadata.input_config_path != rollback.metadata.input_config_path {
        bail!("activation and rollback artifacts must point back to the same source config path");
    }
    Ok(())
}

pub(crate) fn validate_runtime_dir(runtime_dir: &Path) -> Result<()> {
    if !runtime_dir.is_absolute() {
        bail!("runtime dir must be an absolute temp path");
    }
    let temp_dir = env::temp_dir();
    let temp_root = fs::canonicalize(&temp_dir)
        .with_context(|| "failed canonicalizing the system temp root")?;
    let existing_anchor = validate_runtime_dir_existing_anchor(runtime_dir)?;
    let resolved_anchor = fs::canonicalize(&existing_anchor).with_context(|| {
        format!(
            "failed canonicalizing existing runtime-dir anchor {}",
            existing_anchor.display()
        )
    })?;
    if !resolved_anchor.starts_with(&temp_root) {
        bail!(
            "runtime dir {} resolves outside the system temp root {} via existing anchor {}",
            runtime_dir.display(),
            temp_root.display(),
            resolved_anchor.display()
        );
    }
    validate_runtime_dir_descendant_symlinks(runtime_dir, &temp_dir, &temp_root)?;
    if existing_anchor == runtime_dir && resolved_anchor == temp_root {
        bail!("runtime dir must be a child path under the system temp root");
    }
    Ok(())
}

fn validate_runtime_dir_existing_anchor(runtime_dir: &Path) -> Result<PathBuf> {
    let mut current = runtime_dir.to_path_buf();
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if current == runtime_dir && !metadata.is_dir() {
                    bail!(
                        "runtime dir {} already exists and is not a directory",
                        runtime_dir.display()
                    );
                }
                return Ok(current);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current = current.parent().map(Path::to_path_buf).ok_or_else(|| {
                    anyhow!(
                        "runtime dir {} has no existing parent anchor to validate",
                        runtime_dir.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed inspecting runtime-dir path component {}",
                        current.display()
                    )
                });
            }
        }
    }
}

fn validate_runtime_dir_descendant_symlinks(
    runtime_dir: &Path,
    temp_dir: &Path,
    temp_root: &Path,
) -> Result<()> {
    let inspection_root = if runtime_dir.starts_with(temp_dir) {
        temp_dir
    } else if runtime_dir.starts_with(temp_root) {
        temp_root
    } else {
        bail!(
            "runtime dir {} must descend from the system temp root {}",
            runtime_dir.display(),
            temp_root.display()
        );
    };

    let mut current = inspection_root.to_path_buf();
    let relative_components = runtime_dir.strip_prefix(inspection_root).with_context(|| {
        format!(
            "runtime dir {} must stay within the system temp root {}",
            runtime_dir.display(),
            inspection_root.display()
        )
    })?;
    for component in relative_components.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!(
                        "runtime dir path component {} is a symlink; symlinked runtime dirs are not allowed",
                        current.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed inspecting runtime-dir path component {}",
                        current.display()
                    )
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn runtime_paths(runtime_dir: &Path) -> RuntimePaths {
    RuntimePaths {
        runtime_dir: runtime_dir.to_path_buf(),
        session_path: runtime_dir.join("tiny_live_apply_session.json"),
        pid_path: runtime_dir.join("tiny_live_activation.pid"),
        log_path: runtime_dir.join("tiny_live_activation.log"),
        status_path: runtime_dir.join("tiny_live_activation.status.json"),
        stop_request_path: runtime_dir.join("tiny_live_activation.stop"),
        rollback_status_path: runtime_dir.join("tiny_live_rollback.status.json"),
    }
}

fn planned_session(config: &Config, pair: &ValidatedArtifactPair) -> Result<TempRunSession> {
    let paths = runtime_paths(&config.runtime_dir);
    let current_exe = env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "copybot_tiny_live_activation_apply".to_string());
    Ok(TempRunSession {
        session_version: SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        runtime_dir: config.runtime_dir.display().to_string(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: pair.activation.metadata_path.display().to_string(),
        rollback_metadata_path: pair.rollback.metadata_path.display().to_string(),
        source_config_path: pair.activation.metadata.input_config_path.clone(),
        source_config_fingerprint_sha256: pair
            .activation
            .metadata
            .source_config_fingerprint_sha256
            .clone(),
        activation_pre_activation_gate_verdict: pair
            .activation
            .metadata
            .pre_activation_gate_verdict
            .clone(),
        activation_pre_activation_gate_reason: pair
            .activation
            .metadata
            .pre_activation_gate_reason
            .clone(),
        activation_plan_verdict: pair.activation.metadata.activation_plan_verdict.clone(),
        activation_plan_reason: pair.activation.metadata.activation_plan_reason.clone(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        pid_path: paths.pid_path.display().to_string(),
        log_path: paths.log_path.display().to_string(),
        status_path: paths.status_path.display().to_string(),
        stop_request_path: paths.stop_request_path.display().to_string(),
        rollback_status_path: paths.rollback_status_path.display().to_string(),
        apply_command_summary: default_apply_command_summary(config)?,
        verify_command_summary: format!(
            "{} --activation-config {} --rollback-config {} --runtime-dir {} --verify-temp-run",
            current_exe,
            config.activation_config_path.display(),
            config.rollback_config_path.display(),
            config.runtime_dir.display()
        ),
        rollback_command_summary: format!(
            "{} --activation-config {} --rollback-config {} --runtime-dir {} --rollback-temp-run",
            current_exe,
            config.activation_config_path.display(),
            config.rollback_config_path.display(),
            config.runtime_dir.display()
        ),
        explicit_statement:
            "temp apply rehearsal remains isolated and non-authorizing; it does not touch the real prod config or prod service unit"
                .to_string(),
    })
}

fn default_apply_command_summary(config: &Config) -> Result<String> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for temp apply rehearsal")?;
    Ok(format!(
        "{} --activation-config {} --rollback-config {} --runtime-dir {} --apply-temp-run",
        exe.display(),
        config.activation_config_path.display(),
        config.rollback_config_path.display(),
        config.runtime_dir.display()
    ))
}

fn default_worker_command_summary(config: &Config) -> Result<String> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for temp rehearsal worker")?;
    Ok(format!(
        "{} --activation-config {} --rollback-config {} --runtime-dir {} --internal-temp-run-worker",
        exe.display(),
        config.activation_config_path.display(),
        config.rollback_config_path.display(),
        config.runtime_dir.display()
    ))
}

fn default_spawn_worker(request: &WorkerLaunchRequest) -> Result<WorkerLaunch> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for temp rehearsal worker")?;
    if let Some(parent) = request.log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let stdout = fs::File::create(&request.log_path)
        .with_context(|| format!("failed creating worker log {}", request.log_path.display()))?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed cloning worker log {}", request.log_path.display()))?;
    let command_summary = format!(
        "{} --activation-config {} --rollback-config {} --runtime-dir {} --internal-temp-run-worker",
        exe.display(),
        request.activation_config_path.display(),
        request.rollback_config_path.display(),
        request.runtime_dir.display()
    );
    let child = Command::new(&exe)
        .arg("--activation-config")
        .arg(&request.activation_config_path)
        .arg("--rollback-config")
        .arg(&request.rollback_config_path)
        .arg("--runtime-dir")
        .arg(&request.runtime_dir)
        .arg("--internal-temp-run-worker")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed spawning temp rehearsal worker {command_summary}"))?;
    Ok(WorkerLaunch { child })
}

fn wait_for_started_runtime(
    paths: &RuntimePaths,
    session: &TempRunSession,
    startup_timeout_ms: u64,
) -> Result<TempRunVerificationSummary> {
    let deadline = Instant::now() + Duration::from_millis(startup_timeout_ms);
    loop {
        let verification = verify_temp_runtime(paths, session)?;
        if verification.mismatches.is_empty() {
            return Ok(verification);
        }
        if Instant::now() >= deadline {
            return Ok(verification);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(crate) fn verify_temp_runtime(
    paths: &RuntimePaths,
    session: &TempRunSession,
) -> Result<TempRunVerificationSummary> {
    let pid_file_present = paths.pid_path.exists();
    let log_file_present = paths.log_path.exists();
    let status_file_present = paths.status_path.exists();
    let rollback_status_present = paths.rollback_status_path.exists();
    let pid = if pid_file_present {
        Some(read_pid(&paths.pid_path)?)
    } else {
        None
    };
    let process_running = pid.is_some_and(process_is_alive);
    let mut mismatches = Vec::new();
    let status: Option<TempRunStatus> = if status_file_present {
        match fs::read_to_string(&paths.status_path) {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(parsed) => Some(parsed),
                Err(error) => {
                    mismatches.push(format!("runtime status artifact is unreadable: {}", error));
                    None
                }
            },
            Err(error) => {
                mismatches.push(format!(
                    "runtime status artifact could not be read: {}",
                    error
                ));
                None
            }
        }
    } else {
        None
    };
    let heartbeat_fresh = status.as_ref().is_some_and(|status| {
        (Utc::now() - status.last_heartbeat_at)
            .num_milliseconds()
            .max(0) as u64
            <= DEFAULT_HEARTBEAT_STALE_MS
    });

    if !pid_file_present {
        mismatches.push("runtime pid file is missing".to_string());
    }
    if !log_file_present {
        mismatches.push("runtime log file is missing".to_string());
    }
    if !status_file_present {
        mismatches.push("runtime status artifact is missing".to_string());
    }
    if !process_running {
        mismatches.push("temp activation worker is not running".to_string());
    }
    if !heartbeat_fresh {
        mismatches.push("temp activation worker heartbeat is stale".to_string());
    }
    if let Some(status) = &status {
        if status.state != "activation_running" {
            mismatches.push(format!(
                "temp activation runtime state must be activation_running, found {}",
                status.state
            ));
        }
        if status.activation_config_path != session.activation_config_path {
            mismatches.push("status activation_config_path does not match session".to_string());
        }
        if status.rollback_config_path != session.rollback_config_path {
            mismatches.push("status rollback_config_path does not match session".to_string());
        }
        if status.activation_metadata_path != session.activation_metadata_path {
            mismatches.push("status activation_metadata_path does not match session".to_string());
        }
        if status.rollback_metadata_path != session.rollback_metadata_path {
            mismatches.push("status rollback_metadata_path does not match session".to_string());
        }
        if status.source_config_fingerprint_sha256 != session.source_config_fingerprint_sha256 {
            mismatches
                .push("status source_config_fingerprint_sha256 does not match session".to_string());
        }
        if !status.execution_enabled_in_activation_config {
            mismatches.push(
                "status must reflect execution.enabled=true in the activation artifact".to_string(),
            );
        }
        if status.execution_enabled_in_rollback_config {
            mismatches.push(
                "status must reflect execution.enabled=false in the rollback artifact".to_string(),
            );
        }
        if status.activation_authorized {
            mismatches.push("status must remain activation_authorized=false".to_string());
        }
        if let Some(pid_value) = pid {
            if status.pid != pid_value {
                mismatches.push("status pid does not match pid file".to_string());
            }
        }
    }

    Ok(TempRunVerificationSummary {
        pid_file_present,
        log_file_present,
        status_file_present,
        rollback_status_present,
        process_running,
        heartbeat_fresh,
        status_state: status.as_ref().map(|value| value.state.clone()),
        pid,
        mismatches,
    })
}

fn verify_rollback_runtime(
    paths: &RuntimePaths,
    session: &TempRunSession,
    rollback_status: &RollbackStatus,
) -> Result<TempRunVerificationSummary> {
    let pid_file_present = paths.pid_path.exists();
    let log_file_present = paths.log_path.exists();
    let status_file_present = paths.status_path.exists();
    let rollback_status_present = paths.rollback_status_path.exists();
    let pid = if pid_file_present {
        Some(read_pid(&paths.pid_path)?)
    } else {
        None
    };
    let process_running = pid.is_some_and(process_is_alive);
    let mut mismatches = Vec::new();
    let status: Option<TempRunStatus> = if status_file_present {
        match fs::read_to_string(&paths.status_path) {
            Ok(contents) => match serde_json::from_str(&contents) {
                Ok(parsed) => Some(parsed),
                Err(error) => {
                    mismatches.push(format!(
                        "runtime status artifact is unreadable after rollback: {}",
                        error
                    ));
                    None
                }
            },
            Err(error) => {
                mismatches.push(format!(
                    "runtime status artifact could not be read after rollback: {}",
                    error
                ));
                None
            }
        }
    } else {
        None
    };

    if !pid_file_present {
        mismatches.push("runtime pid file is missing".to_string());
    }
    if !log_file_present {
        mismatches.push("runtime log file is missing".to_string());
    }
    if !status_file_present {
        mismatches.push("runtime status artifact is missing".to_string());
    }
    if process_running {
        mismatches.push("temp activation worker is still running after rollback".to_string());
    }
    if !rollback_status_present {
        mismatches.push("rollback status artifact is missing".to_string());
    }
    if let Some(status) = &status {
        if status.state != "activation_stopped" {
            mismatches.push(format!(
                "temp activation runtime state must be activation_stopped after rollback, found {}",
                status.state
            ));
        }
        if status.activation_config_path != session.activation_config_path {
            mismatches.push("status activation_config_path does not match session".to_string());
        }
        if status.rollback_config_path != session.rollback_config_path {
            mismatches.push("status rollback_config_path does not match session".to_string());
        }
        if status.activation_metadata_path != session.activation_metadata_path {
            mismatches.push("status activation_metadata_path does not match session".to_string());
        }
        if status.rollback_metadata_path != session.rollback_metadata_path {
            mismatches.push("status rollback_metadata_path does not match session".to_string());
        }
        if status.source_config_fingerprint_sha256 != session.source_config_fingerprint_sha256 {
            mismatches
                .push("status source_config_fingerprint_sha256 does not match session".to_string());
        }
        if !status.execution_enabled_in_activation_config {
            mismatches.push(
                "status must reflect execution.enabled=true in the activation artifact".to_string(),
            );
        }
        if status.execution_enabled_in_rollback_config {
            mismatches.push(
                "status must reflect execution.enabled=false in the rollback artifact".to_string(),
            );
        }
        if status.activation_authorized {
            mismatches.push("status must remain activation_authorized=false".to_string());
        }
        if let Some(pid_value) = pid {
            if status.pid != pid_value {
                mismatches.push("status pid does not match pid file".to_string());
            }
        }
    }
    if rollback_status.rollback_execution_enabled {
        mismatches.push("rollback artifact must leave execution.enabled=false".to_string());
    }
    if !rollback_status.activation_process_stopped {
        mismatches.push("rollback status does not prove the activation worker stopped".to_string());
    }

    Ok(TempRunVerificationSummary {
        pid_file_present,
        log_file_present,
        status_file_present,
        rollback_status_present,
        process_running,
        heartbeat_fresh: false,
        status_state: status.as_ref().map(|value| value.state.clone()),
        pid,
        mismatches,
    })
}

fn read_pid(path: &Path) -> Result<u32> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading pid file {}", path.display()))?;
    raw.trim()
        .parse::<u32>()
        .with_context(|| format!("invalid pid in {}", path.display()))
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let signal_ok = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !signal_ok {
            return false;
        }
        let ps_state = Command::new("ps")
            .arg("-o")
            .arg("stat=")
            .arg("-p")
            .arg(pid.to_string())
            .output();
        match ps_state {
            Ok(output) if output.status.success() => {
                let state = String::from_utf8_lossy(&output.stdout);
                !state.trim_start().starts_with('Z')
            }
            _ => true,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

pub(crate) fn load_session(path: &Path) -> Result<TempRunSession> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading temp apply session {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing temp apply session {}", path.display()))
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

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("refusing to overwrite existing file {}", path.display());
    }
    Ok(())
}

fn refusal_report(
    config: &Config,
    verdict: TinyLiveActivationApplyVerdict,
    reason: String,
    pair: Option<&ValidatedArtifactPair>,
    verification: Option<TempRunVerificationSummary>,
) -> ApplyReport {
    ApplyReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::Plan => "plan".to_string(),
            Mode::RenderApplyScript => "render_apply_script".to_string(),
            Mode::RenderRollbackScript => "render_rollback_script".to_string(),
            Mode::ApplyTempRun => "apply_temp_run".to_string(),
            Mode::VerifyTempRun => "verify_temp_run".to_string(),
            Mode::RollbackTempRun => "rollback_temp_run".to_string(),
            Mode::InternalTempRunWorker => "internal_temp_run_worker".to_string(),
        },
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        activation_metadata_path: pair.map(|value| value.activation.metadata_path.display().to_string()),
        rollback_metadata_path: pair.map(|value| value.rollback.metadata_path.display().to_string()),
        apply_command_summary: None,
        verify_command_summary: None,
        rollback_command_summary: None,
        temp_run_verification: verification,
        explicit_statement:
            "this bounded rehearsal path remains isolated and non-authorizing; it does not touch the real prod config or prod service"
                .to_string(),
    }
}

fn render_human(report: &ApplyReport) -> String {
    let mut lines = vec![
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("mode={}", report.mode),
        format!("reason={}", report.reason),
        format!("activation_config_path={}", report.activation_config_path),
        format!("rollback_config_path={}", report.rollback_config_path),
        format!("runtime_dir={}", report.runtime_dir),
        format!("startup_timeout_ms={}", report.startup_timeout_ms),
        format!("rollback_timeout_ms={}", report.rollback_timeout_ms),
    ];
    if let Some(path) = &report.activation_metadata_path {
        lines.push(format!("activation_metadata_path={path}"));
    }
    if let Some(path) = &report.rollback_metadata_path {
        lines.push(format!("rollback_metadata_path={path}"));
    }
    if let Some(command) = &report.apply_command_summary {
        lines.push(format!("apply_command_summary={command}"));
    }
    if let Some(command) = &report.verify_command_summary {
        lines.push(format!("verify_command_summary={command}"));
    }
    if let Some(command) = &report.rollback_command_summary {
        lines.push(format!("rollback_command_summary={command}"));
    }
    if let Some(verification) = &report.temp_run_verification {
        lines.push(format!(
            "temp_run_pid_file_present={}",
            verification.pid_file_present
        ));
        lines.push(format!(
            "temp_run_log_file_present={}",
            verification.log_file_present
        ));
        lines.push(format!(
            "temp_run_status_file_present={}",
            verification.status_file_present
        ));
        lines.push(format!(
            "temp_run_rollback_status_present={}",
            verification.rollback_status_present
        ));
        lines.push(format!(
            "temp_run_process_running={}",
            verification.process_running
        ));
        lines.push(format!(
            "temp_run_heartbeat_fresh={}",
            verification.heartbeat_fresh
        ));
        if let Some(state) = &verification.status_state {
            lines.push(format!("temp_run_status_state={state}"));
        }
        if let Some(pid) = verification.pid {
            lines.push(format!("temp_run_pid={pid}"));
        }
        if !verification.mismatches.is_empty() {
            lines.push("temp_run_mismatches:".to_string());
            lines.extend(
                verification
                    .mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
    }
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
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
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid u64 for {flag}: {raw}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn valid_rendered_artifacts_produce_a_bounded_apply_plan() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_plan");
        let runtime_dir = temp_dir("tiny_live_activation_apply_runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );

        let report = build_plan_report(&sample_config(
            Mode::Plan,
            activation_config,
            rollback_config,
            runtime_dir,
            None,
        ))
        .expect("plan");

        assert_eq!(
            report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyPlanReady
        );
        assert!(report.apply_command_summary.is_some());
        assert!(report.rollback_command_summary.is_some());
    }

    #[test]
    fn invalid_or_drifted_rendered_metadata_is_refused_explicitly() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_invalid_artifact");
        let runtime_dir = temp_dir("tiny_live_activation_apply_invalid_runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );
        fs::write(
            tiny_live_activation_execute::metadata_path_for_rendered_config(&activation_config),
            "{invalid json",
        )
        .unwrap();

        let report = build_plan_report(&sample_config(
            Mode::Plan,
            activation_config,
            rollback_config,
            runtime_dir,
            None,
        ))
        .expect("plan");

        assert_eq!(
            report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByInvalidRenderedArtifact
        );
    }

    #[test]
    fn prod_like_runtime_paths_are_refused() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_prod_like");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );
        let report = build_plan_report(&sample_config(
            Mode::Plan,
            activation_config,
            rollback_config,
            PathBuf::from("/etc/solana-copy-bot/rehearsal"),
            None,
        ))
        .expect("plan");

        assert_eq!(
            report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_runtime_dir_escape_is_rejected_without_outside_artifact_writes() {
        use std::os::unix::fs::symlink;

        let fixture_dir = temp_dir("tiny_live_activation_apply_symlink_escape");
        let runtime_link = fixture_dir.join("runtime-link");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        let outside_dir = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!(
                "tiny_live_activation_apply_outside_{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        fs::create_dir_all(&outside_dir).unwrap();
        symlink(&outside_dir, &runtime_link).unwrap();
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );

        let report = apply_temp_run_with_spawner(
            &sample_config(
                Mode::ApplyTempRun,
                activation_config,
                rollback_config,
                runtime_link.join("child"),
                None,
            ),
            |request| spawn_script_worker(request),
        )
        .unwrap();

        assert_eq!(
            report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyRefusedByUnsafeRuntimeDir
        );
        assert!(
            report.reason.contains("symlink"),
            "unexpected refusal reason: {}",
            report.reason
        );
        assert!(!outside_dir.join("tiny_live_apply_session.json").exists());
        assert!(!outside_dir.join("tiny_live_activation.pid").exists());
        assert!(!outside_dir.join("tiny_live_activation.log").exists());
        assert!(!outside_dir
            .join("tiny_live_activation.status.json")
            .exists());
        assert!(!outside_dir.join("tiny_live_rollback.status.json").exists());
        fs::remove_dir_all(&outside_dir).ok();
    }

    #[test]
    fn rendered_rehearsal_scripts_use_public_temp_modes_only() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_scripts");
        let runtime_dir = fixture_dir.join("runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        let apply_script = fixture_dir.join("apply.sh");
        let rollback_script = fixture_dir.join("rollback.sh");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );

        let apply_report = render_script_report(
            &sample_config(
                Mode::RenderApplyScript,
                activation_config.clone(),
                rollback_config.clone(),
                runtime_dir.clone(),
                Some(apply_script.clone()),
            ),
            ScriptKind::Apply,
        )
        .unwrap();
        let rollback_report = render_script_report(
            &sample_config(
                Mode::RenderRollbackScript,
                activation_config,
                rollback_config,
                runtime_dir,
                Some(rollback_script.clone()),
            ),
            ScriptKind::Rollback,
        )
        .unwrap();

        assert_eq!(
            apply_report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationApplyScriptRendered
        );
        assert_eq!(
            rollback_report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveRollbackApplyScriptRendered
        );

        let apply_contents = fs::read_to_string(&apply_script).unwrap();
        let rollback_contents = fs::read_to_string(&rollback_script).unwrap();
        assert!(apply_contents.contains("--apply-temp-run"));
        assert!(!apply_contents.contains("--internal-temp-run-worker"));
        assert!(rollback_contents.contains("--rollback-temp-run"));
        assert!(!rollback_contents.contains("--internal-temp-run-worker"));
        assert!(!apply_contents.contains("systemctl"));
        assert!(!rollback_contents.contains("systemctl"));
        assert!(!apply_contents.contains("/etc/solana-copy-bot/live.server.toml"));
        assert!(!rollback_contents.contains("/etc/solana-copy-bot/live.server.toml"));
    }

    #[test]
    fn temp_apply_mode_writes_bounded_pid_log_and_status_artifacts_only_inside_runtime_dir() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_runtime_artifacts");
        let runtime_dir = fixture_dir.join("runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );

        let report = apply_temp_run_with_spawner(
            &sample_config(
                Mode::ApplyTempRun,
                activation_config.clone(),
                rollback_config.clone(),
                runtime_dir.clone(),
                None,
            ),
            |request| spawn_script_worker(request),
        )
        .expect("apply");

        assert_eq!(
            report.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunStarted
        );
        let paths = runtime_paths(&runtime_dir);
        assert!(paths.pid_path.exists());
        assert!(paths.log_path.exists());
        assert!(paths.status_path.exists());
        assert!(paths.pid_path.starts_with(&runtime_dir));
        assert!(paths.log_path.starts_with(&runtime_dir));
        assert!(paths.status_path.starts_with(&runtime_dir));
    }

    #[test]
    fn temp_verify_catches_missing_or_stale_runtime_artifacts() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_verify_stale");
        let runtime_dir = fixture_dir.join("runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );
        let config = sample_config(
            Mode::ApplyTempRun,
            activation_config.clone(),
            rollback_config.clone(),
            runtime_dir.clone(),
            None,
        );
        apply_temp_run_with_spawner(&config, |request| spawn_script_worker(request)).unwrap();
        let paths = runtime_paths(&runtime_dir);
        let mut status: TempRunStatus =
            serde_json::from_str(&fs::read_to_string(&paths.status_path).unwrap()).unwrap();
        status.last_heartbeat_at = Utc::now() - chrono::Duration::seconds(60);
        fs::write(
            &paths.status_path,
            serde_json::to_string_pretty(&status).unwrap(),
        )
        .unwrap();

        let verify = verify_temp_run(&Config {
            mode: Mode::VerifyTempRun,
            activation_config_path: activation_config,
            rollback_config_path: rollback_config,
            runtime_dir,
            output_path: None,
            startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
            rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
            json: false,
        })
        .unwrap();

        assert_eq!(
            verify.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveActivationTempRunVerifyFailed
        );
        assert!(verify
            .temp_run_verification
            .as_ref()
            .unwrap()
            .mismatches
            .iter()
            .any(|entry| entry.contains("heartbeat is stale")));
    }

    #[test]
    fn rollback_temp_mode_deterministically_restores_disabled_posture() {
        let fixture_dir = temp_dir("tiny_live_activation_apply_rollback");
        let runtime_dir = fixture_dir.join("runtime");
        let activation_config = fixture_dir.join("activation.toml");
        let rollback_config = fixture_dir.join("rollback.toml");
        write_rendered_artifact(
            &activation_config,
            tiny_live_activation_execute::RenderKind::Activation,
            sample_activation_config_toml(),
            true,
        );
        write_rendered_artifact(
            &rollback_config,
            tiny_live_activation_execute::RenderKind::Rollback,
            sample_rollback_config_toml(),
            false,
        );
        let apply_config = sample_config(
            Mode::ApplyTempRun,
            activation_config.clone(),
            rollback_config.clone(),
            runtime_dir.clone(),
            None,
        );
        apply_temp_run_with_spawner(&apply_config, |request| spawn_script_worker(request)).unwrap();

        let rollback = rollback_temp_run(&Config {
            mode: Mode::RollbackTempRun,
            activation_config_path: activation_config,
            rollback_config_path: rollback_config.clone(),
            runtime_dir: runtime_dir.clone(),
            output_path: None,
            startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
            rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
            json: false,
        })
        .unwrap();

        assert_eq!(
            rollback.verdict,
            TinyLiveActivationApplyVerdict::TinyLiveRollbackTempRunCompleted
        );
        let paths = runtime_paths(&runtime_dir);
        let rollback_status: RollbackStatus =
            serde_json::from_str(&fs::read_to_string(&paths.rollback_status_path).unwrap())
                .unwrap();
        assert!(!rollback_status.rollback_execution_enabled);
        assert!(rollback_status.activation_process_stopped);
    }

    fn sample_config(
        mode: Mode,
        activation_config_path: PathBuf,
        rollback_config_path: PathBuf,
        runtime_dir: PathBuf,
        output_path: Option<PathBuf>,
    ) -> Config {
        Config {
            mode,
            activation_config_path,
            rollback_config_path,
            runtime_dir,
            output_path,
            startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
            rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
            json: false,
        }
    }

    fn write_rendered_artifact(
        path: &Path,
        render_kind: tiny_live_activation_execute::RenderKind,
        contents: String,
        activation_enabled: bool,
    ) {
        fs::write(path, &contents).unwrap();
        let hash = format!("{:x}", Sha256::digest(contents.as_bytes()));
        let metadata = tiny_live_activation_execute::RenderedConfigMetadata {
            metadata_version: "1".to_string(),
            render_kind,
            generated_at: ts("2026-03-27T12:00:00Z"),
            input_config_path: "/tmp/source.live.server.toml".to_string(),
            output_config_path: path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_execute::FINGERPRINT_SCOPE
                .to_string(),
            source_config_fingerprint_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            expected_source_fingerprint_sha256: None,
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
                source_value: serde_json::json!(!activation_enabled),
                target_value: serde_json::json!(activation_enabled),
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

    fn sample_activation_config_toml() -> String {
        r#"
[system]
env = "prod-live"

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

    fn sample_rollback_config_toml() -> String {
        sample_activation_config_toml().replace("enabled = true", "enabled = false")
    }

    fn spawn_script_worker(request: &WorkerLaunchRequest) -> Result<WorkerLaunch> {
        let script_path = request.runtime_dir.join("worker.sh");
        let status_path = runtime_paths(&request.runtime_dir).status_path;
        let stop_request_path = runtime_paths(&request.runtime_dir).stop_request_path;
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nruntime_dir=\"$1\"\nactivation_config=\"$2\"\nrollback_config=\"$3\"\nstatus_path=\"{}\"\nstop_path=\"{}\"\nwhile true; do\n  now=$(date -u +\"%Y-%m-%dT%H:%M:%SZ\")\n  tmp_status=\"${{status_path}}.tmp.$$\"\n  cat > \"$tmp_status\" <<JSON\n{{\n  \"status_version\": \"1\",\n  \"state\": \"$( [ -f \"$stop_path\" ] && echo activation_stopped || echo activation_running )\",\n  \"started_at\": \"$now\",\n  \"last_heartbeat_at\": \"$now\",\n  \"stopped_at\": null,\n  \"pid\": $$,\n  \"runtime_dir\": \"$runtime_dir\",\n  \"command_summary\": \"test-worker\",\n  \"activation_config_path\": \"$activation_config\",\n  \"rollback_config_path\": \"$rollback_config\",\n  \"activation_metadata_path\": \"{}\",\n  \"rollback_metadata_path\": \"{}\",\n  \"source_config_fingerprint_sha256\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\n  \"execution_enabled_in_activation_config\": true,\n  \"execution_enabled_in_rollback_config\": false,\n  \"activation_authorized\": false,\n  \"note\": \"test worker\"\n}}\nJSON\n  mv \"$tmp_status\" \"$status_path\"\n  if [ -f \"$stop_path\" ]; then\n    exit 0\n  fi\n  sleep 0.2\ndone\n",
            status_path.display(),
            stop_request_path.display(),
            tiny_live_activation_execute::metadata_path_for_rendered_config(
                &request.activation_config_path
            )
            .display(),
            tiny_live_activation_execute::metadata_path_for_rendered_config(
                &request.rollback_config_path
            )
            .display(),
        );
        fs::write(&script_path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&script_path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&script_path, perms).unwrap();
        }
        let stdout = fs::File::create(&request.log_path).unwrap();
        let stderr = stdout.try_clone().unwrap();
        let child = Command::new("/bin/bash")
            .arg(&script_path)
            .arg(&request.runtime_dir)
            .arg(&request.activation_config_path)
            .arg(&request.rollback_config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap();
        Ok(WorkerLaunch { child })
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
}
