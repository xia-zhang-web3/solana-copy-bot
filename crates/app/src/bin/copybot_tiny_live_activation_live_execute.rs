use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "copybot_activation_decision_packet.rs"]
pub(crate) mod activation_decision_packet;
#[allow(dead_code)]
#[path = "../live_service_control_wrapper_contract.rs"]
pub(crate) mod live_service_control_wrapper_contract;
#[allow(dead_code)]
#[path = "copybot_pre_activation_gate_report.rs"]
pub(crate) mod pre_activation_gate_report;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_execute.rs"]
pub(crate) mod tiny_live_activation_execute;

const USAGE: &str = "usage: copybot_tiny_live_activation_live_execute --activation-config <path> --rollback-config <path> --target-config <path> --target-service <name> --service-control-command <path> --runtime-dir <path> --backup-dir <path> [--output-dir <path>] [--output <path>] [--json] [--startup-timeout-ms <ms>] [--rollback-timeout-ms <ms>] (--plan-live | --backup-current-config | --render-live-apply-script | --render-live-rollback-script | --verify-live-target | --apply-live | --rollback-live)";
const BACKUP_METADATA_VERSION: &str = "1";
const SESSION_VERSION: &str = "1";
const STATUS_VERSION: &str = "1";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_ROLLBACK_TIMEOUT_MS: u64 = 10_000;

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
    target_config_path: PathBuf,
    target_service_name: String,
    service_control_command_path: PathBuf,
    runtime_dir: PathBuf,
    backup_dir: PathBuf,
    output_path: Option<PathBuf>,
    json: bool,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLive,
    BackupCurrentConfig,
    RenderLiveApplyScript,
    RenderLiveRollbackScript,
    VerifyLiveTarget,
    ApplyLive,
    RollbackLive,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TinyLiveLiveExecuteVerdict {
    TinyLiveLivePlanReady,
    TinyLiveLiveBackupCreated,
    TinyLiveLiveBackupRefused,
    TinyLiveLiveApplyScriptRendered,
    TinyLiveLiveRollbackScriptRendered,
    TinyLiveLiveVerifyOk,
    TinyLiveLiveVerifyInvalid,
    TinyLiveLiveApplyRefusedByStage3,
    TinyLiveLiveApplyRefusedByPreActivationGate,
    TinyLiveLiveApplyRefusedByConfigDrift,
    TinyLiveLiveApplyRefusedByInvalidArtifact,
    TinyLiveLiveApplyRefusedByMissingBackup,
    TinyLiveLiveApplyRefusedByUnsafeTarget,
    TinyLiveLiveApplyStarted,
    TinyLiveLiveApplyRolledBack,
    TinyLiveLiveRollbackCompleted,
    TinyLiveLiveRollbackVerifyFailed,
}

#[derive(Debug, Clone)]
struct ValidatedArtifactPair {
    activation: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
    rollback: tiny_live_activation_execute::LoadedRenderedConfigArtifact,
}

#[derive(Debug, Clone)]
pub(crate) struct TargetContract {
    pub(crate) current_bytes: Vec<u8>,
    pub(crate) current_fingerprint: activation_decision_packet::ConfigFingerprintSummary,
    pub(crate) service_control_wrapper_verification: ServiceControlWrapperVerificationSummary,
}

pub(crate) type ServiceControlWrapperVerificationSummary =
    live_service_control_wrapper_contract::WrapperVerificationSummary;

#[derive(Debug, Clone)]
struct CurrentGateTruth {
    report: pre_activation_gate_report::PreActivationGateReport,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveRuntimePaths {
    pub(crate) runtime_dir: PathBuf,
    pub(crate) session_path: PathBuf,
    pub(crate) apply_status_path: PathBuf,
    pub(crate) rollback_status_path: PathBuf,
    pub(crate) log_path: PathBuf,
}

#[derive(Debug, Clone)]
struct BackupPaths {
    backup_config_path: PathBuf,
    backup_metadata_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveBackupMetadata {
    pub(crate) metadata_version: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) target_service_name: String,
    pub(crate) target_config_path: String,
    pub(crate) backed_up_config_path: String,
    pub(crate) source_config_fingerprint_scope: String,
    pub(crate) source_config_fingerprint_sha256: String,
    pub(crate) backed_up_config_sha256: String,
    pub(crate) activation_config_path: String,
    pub(crate) activation_metadata_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) rollback_metadata_path: String,
    pub(crate) explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    activation_config_path: String,
    rollback_config_path: String,
    activation_metadata_path: String,
    rollback_metadata_path: String,
    target_config_path: String,
    target_service_name: String,
    service_control_command_path: String,
    runtime_dir: String,
    backup_dir: String,
    backup_metadata_path: Option<String>,
    source_config_fingerprint_sha256: String,
    source_config_fingerprint_scope: String,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
    apply_command_summary: String,
    verify_command_summary: String,
    rollback_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveServiceStatus {
    pub(crate) status_version: String,
    pub(crate) action: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) service_name: String,
    pub(crate) target_config_path: String,
    pub(crate) expected_config_fingerprint_sha256: String,
    pub(crate) observed_config_fingerprint_sha256: String,
    pub(crate) expected_execution_enabled: bool,
    pub(crate) observed_execution_enabled: bool,
    pub(crate) restart_successful: bool,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveTargetVerificationSummary {
    pub(crate) target_config_path_matches_rendered_source: bool,
    pub(crate) current_target_fingerprint_matches_rendered_source: Option<bool>,
    pub(crate) backup_proof_present: Option<bool>,
    pub(crate) status_artifact_present: bool,
    pub(crate) status_restart_successful: Option<bool>,
    pub(crate) observed_execution_enabled: Option<bool>,
    pub(crate) observed_config_fingerprint_sha256: Option<String>,
    pub(crate) service_control_wrapper_verification:
        Option<ServiceControlWrapperVerificationSummary>,
    pub(crate) mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveExecuteReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) mode: String,
    pub(crate) verdict: TinyLiveLiveExecuteVerdict,
    pub(crate) reason: String,
    pub(crate) activation_config_path: String,
    pub(crate) rollback_config_path: String,
    pub(crate) activation_metadata_path: Option<String>,
    pub(crate) rollback_metadata_path: Option<String>,
    pub(crate) target_config_path: String,
    pub(crate) target_service_name: String,
    pub(crate) service_control_command_path: String,
    pub(crate) runtime_dir: String,
    pub(crate) backup_dir: String,
    pub(crate) backup_config_path: Option<String>,
    pub(crate) backup_metadata_path: Option<String>,
    pub(crate) pre_activation_gate_verdict_used: Option<String>,
    pub(crate) pre_activation_gate_reason_used: Option<String>,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) activation_plan_verdict_used: Option<String>,
    pub(crate) activation_plan_reason_used: Option<String>,
    pub(crate) current_target_fingerprint_scope: Option<String>,
    pub(crate) current_target_fingerprint_sha256: Option<String>,
    pub(crate) rendered_source_fingerprint_sha256: Option<String>,
    pub(crate) changed_fields: Vec<tiny_live_activation_execute::FieldExpectation>,
    pub(crate) backup_command_summary: Option<String>,
    pub(crate) apply_command_summary: Option<String>,
    pub(crate) verify_command_summary: Option<String>,
    pub(crate) rollback_command_summary: Option<String>,
    pub(crate) live_verification: Option<LiveTargetVerificationSummary>,
    pub(crate) activation_authorized: bool,
    pub(crate) explicit_statement: String,
}

#[derive(Debug, Clone)]
struct CommandSummaries {
    backup_command_summary: String,
    apply_command_summary: String,
    verify_command_summary: String,
    rollback_command_summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScriptKind {
    Apply,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceAction {
    Activation,
    Rollback,
}

impl ServiceAction {
    fn as_str(self) -> &'static str {
        match self {
            ServiceAction::Activation => "activation",
            ServiceAction::Rollback => "rollback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContractFailureKind {
    InvalidArtifact,
    UnsafeTarget,
    ConfigDrift,
    Stage3,
    PreActivationGate,
    MissingBackup,
}

#[derive(Debug, Clone)]
struct ContractFailure {
    kind: ContractFailureKind,
    reason: String,
}

impl ContractFailure {
    fn new(kind: ContractFailureKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
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
    let mut activation_config_path: Option<PathBuf> = None;
    let mut rollback_config_path: Option<PathBuf> = None;
    let mut target_config_path: Option<PathBuf> = None;
    let mut target_service_name: Option<String> = None;
    let mut service_control_command_path: Option<PathBuf> = None;
    let mut runtime_dir: Option<PathBuf> = None;
    let mut backup_dir: Option<PathBuf> = None;
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
            "--runtime-dir" => {
                runtime_dir = Some(PathBuf::from(parse_string_arg(
                    "--runtime-dir",
                    args.next(),
                )?))
            }
            "--backup-dir" | "--output-dir" => {
                backup_dir = Some(PathBuf::from(parse_string_arg(arg.as_str(), args.next())?))
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
            "--plan-live" => set_mode(&mut mode, Mode::PlanLive, "--plan-live")?,
            "--backup-current-config" => set_mode(
                &mut mode,
                Mode::BackupCurrentConfig,
                "--backup-current-config",
            )?,
            "--render-live-apply-script" => set_mode(
                &mut mode,
                Mode::RenderLiveApplyScript,
                "--render-live-apply-script",
            )?,
            "--render-live-rollback-script" => set_mode(
                &mut mode,
                Mode::RenderLiveRollbackScript,
                "--render-live-rollback-script",
            )?,
            "--verify-live-target" => {
                set_mode(&mut mode, Mode::VerifyLiveTarget, "--verify-live-target")?
            }
            "--apply-live" => set_mode(&mut mode, Mode::ApplyLive, "--apply-live")?,
            "--rollback-live" => set_mode(&mut mode, Mode::RollbackLive, "--rollback-live")?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| {
        anyhow!(
            "select exactly one mode: --plan-live | --backup-current-config | --render-live-apply-script | --render-live-rollback-script | --verify-live-target | --apply-live | --rollback-live"
        )
    })?;
    let activation_config_path =
        activation_config_path.ok_or_else(|| anyhow!("missing required --activation-config"))?;
    let rollback_config_path =
        rollback_config_path.ok_or_else(|| anyhow!("missing required --rollback-config"))?;
    let target_config_path =
        target_config_path.ok_or_else(|| anyhow!("missing required --target-config"))?;
    let target_service_name =
        target_service_name.ok_or_else(|| anyhow!("missing required --target-service"))?;
    let service_control_command_path = service_control_command_path
        .ok_or_else(|| anyhow!("missing required --service-control-command"))?;
    let runtime_dir = runtime_dir.ok_or_else(|| anyhow!("missing required --runtime-dir"))?;
    let backup_dir = backup_dir.ok_or_else(|| anyhow!("missing required --backup-dir"))?;

    match mode {
        Mode::RenderLiveApplyScript | Mode::RenderLiveRollbackScript => {
            if output_path.is_none() {
                bail!("--output is required for live script render modes");
            }
        }
        _ => {
            if output_path.is_some() {
                bail!("--output is only valid with live script render modes");
            }
        }
    }

    Ok(Some(Config {
        mode,
        activation_config_path,
        rollback_config_path,
        target_config_path,
        target_service_name,
        service_control_command_path,
        runtime_dir,
        backup_dir,
        output_path,
        json,
        startup_timeout_ms: startup_timeout_ms.max(1_000),
        rollback_timeout_ms: rollback_timeout_ms.max(1_000),
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLive => build_plan_live_report(&config),
        Mode::BackupCurrentConfig => backup_current_config_report(&config),
        Mode::RenderLiveApplyScript => render_live_script_report(&config, ScriptKind::Apply),
        Mode::RenderLiveRollbackScript => render_live_script_report(&config, ScriptKind::Rollback),
        Mode::VerifyLiveTarget => verify_live_target_report(&config),
        Mode::ApplyLive => apply_live_report(&config),
        Mode::RollbackLive => rollback_live_report(&config),
    }?;
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

fn build_plan_live_report(config: &Config) -> Result<LiveExecuteReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                None,
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    let target = match inspect_target_contract(config, &pair) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                Some(&pair),
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    if let Err(failure) = require_source_fingerprint_match(&pair, &target) {
        return Ok(report_for_failure(
            config,
            Some(&pair),
            Some(&target),
            None,
            failure,
            config.mode,
        ));
    }
    let current_gate = match inspect_current_live_gate(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                Some(&pair),
                Some(&target),
                None,
                failure,
                config.mode,
            ))
        }
    };
    if let Err(failure) = require_live_gate(&pair, &current_gate) {
        return Ok(report_for_failure(
            config,
            Some(&pair),
            Some(&target),
            Some(&current_gate),
            failure,
            config.mode,
        ));
    }
    let summaries = build_command_summaries(config)?;
    let backup_paths = backup_artifact_paths(config, &target)?;
    let backup_exists = backup_paths.backup_metadata_path.exists();
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "plan_live".to_string(),
        verdict: TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady,
        reason: if backup_exists {
            "bounded live-target activation/rollback contract is explicit and a matching backup already exists; production activation still remains separately blocked until an explicit later decision".to_string()
        } else {
            "bounded live-target activation/rollback contract is explicit; create a deterministic backup before any later live-target mutation".to_string()
        },
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: Some(backup_paths.backup_config_path.display().to_string()),
        backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
        pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: Some(serialize_enum(&current_gate.report.verdict)),
        current_pre_activation_gate_reason: Some(current_gate.report.reason.clone()),
        activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: Some(
            pair.activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
        ),
        changed_fields: pair.activation.metadata.field_expectations.clone(),
        backup_command_summary: Some(summaries.backup_command_summary),
        apply_command_summary: Some(summaries.apply_command_summary),
        verify_command_summary: Some(summaries.verify_command_summary),
        rollback_command_summary: Some(summaries.rollback_command_summary),
        live_verification: Some(LiveTargetVerificationSummary {
            target_config_path_matches_rendered_source: true,
            current_target_fingerprint_matches_rendered_source: Some(true),
            backup_proof_present: Some(backup_exists),
            status_artifact_present: false,
            status_restart_successful: None,
            observed_execution_enabled: None,
            observed_config_fingerprint_sha256: None,
            service_control_wrapper_verification: Some(
                target.service_control_wrapper_verification.clone(),
            ),
            mismatches: Vec::new(),
        }),
        activation_authorized: false,
        explicit_statement:
            "this batch builds a bounded live-target executor path only; it does not authorize or perform real production activation on the live server by itself"
                .to_string(),
    })
}

fn backup_current_config_report(config: &Config) -> Result<LiveExecuteReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_backup_failure(
                config,
                None,
                None,
                failure.reason,
            ))
        }
    };
    let target = match inspect_target_contract(config, &pair) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_backup_failure(
                config,
                Some(&pair),
                None,
                failure.reason,
            ))
        }
    };
    let paths = backup_artifact_paths(config, &target)?;
    if paths.backup_config_path.exists() || paths.backup_metadata_path.exists() {
        return Ok(report_for_backup_failure(
            config,
            Some(&pair),
            Some(&target),
            format!(
                "refusing to overwrite existing live-target backup {}",
                paths.backup_config_path.display()
            ),
        ));
    }
    if let Some(parent) = paths.backup_config_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating live-target backup dir {}",
                parent.display()
            )
        })?;
    }
    fs::write(&paths.backup_config_path, &target.current_bytes).with_context(|| {
        format!(
            "failed writing live-target backup {}",
            paths.backup_config_path.display()
        )
    })?;
    let metadata = LiveBackupMetadata {
        metadata_version: BACKUP_METADATA_VERSION.to_string(),
        created_at: Utc::now(),
        target_service_name: config.target_service_name.clone(),
        target_config_path: config.target_config_path.display().to_string(),
        backed_up_config_path: paths.backup_config_path.display().to_string(),
        source_config_fingerprint_scope: target.current_fingerprint.scope.clone(),
        source_config_fingerprint_sha256: target.current_fingerprint.sha256.clone(),
        backed_up_config_sha256: sha256_hex(&target.current_bytes),
        activation_config_path: config.activation_config_path.display().to_string(),
        activation_metadata_path: pair.activation.metadata_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        rollback_metadata_path: pair.rollback.metadata_path.display().to_string(),
        explicit_statement:
            "live-target backup captures the current config only; it does not authorize or perform production activation by itself"
                .to_string(),
    };
    write_text_file(
        &paths.backup_metadata_path,
        &serde_json::to_string_pretty(&metadata)?,
    )?;
    let summaries = build_command_summaries(config)?;
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "backup_current_config".to_string(),
        verdict: TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated,
        reason: "deterministic live-target config backup created without mutating the target service or enabling execution".to_string(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: Some(paths.backup_config_path.display().to_string()),
        backup_metadata_path: Some(paths.backup_metadata_path.display().to_string()),
        pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: Some(
            pair.activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
        ),
        changed_fields: pair.activation.metadata.field_expectations.clone(),
        backup_command_summary: Some(summaries.backup_command_summary),
        apply_command_summary: Some(summaries.apply_command_summary),
        verify_command_summary: Some(summaries.verify_command_summary),
        rollback_command_summary: Some(summaries.rollback_command_summary),
        live_verification: Some(LiveTargetVerificationSummary {
            target_config_path_matches_rendered_source: target_path_matches_artifacts(config, &pair),
            current_target_fingerprint_matches_rendered_source: Some(
                target.current_fingerprint.sha256
                    == pair.activation.metadata.source_config_fingerprint_sha256,
            ),
            backup_proof_present: Some(true),
            status_artifact_present: false,
            status_restart_successful: None,
            observed_execution_enabled: None,
            observed_config_fingerprint_sha256: None,
            service_control_wrapper_verification: Some(
                target.service_control_wrapper_verification.clone(),
            ),
            mismatches: Vec::new(),
        }),
        activation_authorized: false,
        explicit_statement:
            "backup mode prepares a deterministic recovery artifact only; it does not authorize or perform production activation by itself"
                .to_string(),
    })
}

fn render_live_script_report(config: &Config, kind: ScriptKind) -> Result<LiveExecuteReport> {
    let plan = build_plan_live_report(config)?;
    if plan.verdict != TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("live script render modes require --output"))?;
    ensure_new_output_path(output_path)?;
    let command = match kind {
        ScriptKind::Apply => plan
            .apply_command_summary
            .clone()
            .ok_or_else(|| anyhow!("missing live apply command summary"))?,
        ScriptKind::Rollback => plan
            .rollback_command_summary
            .clone()
            .ok_or_else(|| anyhow!("missing live rollback command summary"))?,
    };
    write_text_file(
        output_path,
        &format!("#!/usr/bin/env bash\nset -euo pipefail\n{command}\n"),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: match kind {
            ScriptKind::Apply => "render_live_apply_script".to_string(),
            ScriptKind::Rollback => "render_live_rollback_script".to_string(),
        },
        verdict: match kind {
            ScriptKind::Apply => TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyScriptRendered,
            ScriptKind::Rollback => TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackScriptRendered,
        },
        reason: match kind {
            ScriptKind::Apply => {
                "bounded live-target activation script rendered; Stage 3 and pre-activation gates still remain hard blockers at apply time".to_string()
            }
            ScriptKind::Rollback => {
                "bounded live-target rollback script rendered over the same deterministic artifact pair".to_string()
            }
        },
        activation_config_path: plan.activation_config_path,
        rollback_config_path: plan.rollback_config_path,
        activation_metadata_path: plan.activation_metadata_path,
        rollback_metadata_path: plan.rollback_metadata_path,
        target_config_path: plan.target_config_path,
        target_service_name: plan.target_service_name,
        service_control_command_path: plan.service_control_command_path,
        runtime_dir: plan.runtime_dir,
        backup_dir: plan.backup_dir,
        backup_config_path: plan.backup_config_path,
        backup_metadata_path: plan.backup_metadata_path,
        pre_activation_gate_verdict_used: plan.pre_activation_gate_verdict_used,
        pre_activation_gate_reason_used: plan.pre_activation_gate_reason_used,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        activation_plan_verdict_used: plan.activation_plan_verdict_used,
        activation_plan_reason_used: plan.activation_plan_reason_used,
        current_target_fingerprint_scope: plan.current_target_fingerprint_scope,
        current_target_fingerprint_sha256: plan.current_target_fingerprint_sha256,
        rendered_source_fingerprint_sha256: plan.rendered_source_fingerprint_sha256,
        changed_fields: plan.changed_fields,
        backup_command_summary: plan.backup_command_summary,
        apply_command_summary: plan.apply_command_summary,
        verify_command_summary: plan.verify_command_summary,
        rollback_command_summary: plan.rollback_command_summary,
        live_verification: plan.live_verification,
        activation_authorized: false,
        explicit_statement:
            "rendered live-target scripts only codify the bounded executor contract; they do not authorize or perform real production activation by themselves"
                .to_string(),
    })
}

fn verify_live_target_report(config: &Config) -> Result<LiveExecuteReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_verify_invalid(
                config,
                None,
                None,
                None,
                failure.reason,
            ))
        }
    };
    let target = match inspect_target_contract(config, &pair) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_verify_invalid(
                config,
                Some(&pair),
                None,
                None,
                failure.reason,
            ))
        }
    };
    let mut mismatches = Vec::new();
    if !target_path_matches_artifacts(config, &pair) {
        mismatches.push(format!(
            "rendered artifacts point to {} but live-target verify was asked to operate on {}",
            pair.activation.metadata.input_config_path,
            config.target_config_path.display()
        ));
    }
    if target.current_fingerprint.sha256
        != pair.activation.metadata.source_config_fingerprint_sha256
    {
        mismatches.push(format!(
            "current live config fingerprint {} does not match rendered artifact source fingerprint {}",
            target.current_fingerprint.sha256,
            pair.activation.metadata.source_config_fingerprint_sha256
        ));
    }
    let current_gate = match inspect_current_live_gate(config) {
        Ok(value) => Some(value),
        Err(failure) => {
            mismatches.push(failure.reason);
            None
        }
    };
    if let Some(current_gate) = &current_gate {
        if current_gate.report.verdict
            != pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen
        {
            mismatches.push(format!(
                "current pre-activation gate is non-green for live-target activation: {} ({})",
                serialize_enum(&current_gate.report.verdict),
                current_gate.report.reason
            ));
        }
    }
    if let Err(failure) = validate_live_plan_contract(&pair) {
        mismatches.push(failure.reason);
    }
    let backup_paths = backup_artifact_paths(config, &target)?;
    let backup_proof_present = if backup_paths.backup_metadata_path.exists() {
        match load_backup_proof(config, &backup_paths, &pair, &target) {
            Ok(_) => Some(true),
            Err(failure) => {
                mismatches.push(failure.reason);
                Some(false)
            }
        }
    } else {
        Some(false)
    };
    let summaries = build_command_summaries(config)?;
    let verification = LiveTargetVerificationSummary {
        target_config_path_matches_rendered_source: target_path_matches_artifacts(config, &pair),
        current_target_fingerprint_matches_rendered_source: Some(
            target.current_fingerprint.sha256
                == pair.activation.metadata.source_config_fingerprint_sha256,
        ),
        backup_proof_present,
        status_artifact_present: false,
        status_restart_successful: None,
        observed_execution_enabled: None,
        observed_config_fingerprint_sha256: None,
        service_control_wrapper_verification: Some(
            target.service_control_wrapper_verification.clone(),
        ),
        mismatches: mismatches.clone(),
    };
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "verify_live_target".to_string(),
        verdict: if mismatches.is_empty() {
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyOk
        } else {
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyInvalid
        },
        reason: if mismatches.is_empty() {
            "live-target activation/rollback contract is structurally valid and bounded; a later live apply still remains separately gated".to_string()
        } else {
            mismatches
                .first()
                .cloned()
                .unwrap_or_else(|| "live-target verification failed".to_string())
        },
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: Some(backup_paths.backup_config_path.display().to_string()),
        backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
        pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: current_gate
            .as_ref()
            .map(|value| serialize_enum(&value.report.verdict)),
        current_pre_activation_gate_reason: current_gate
            .as_ref()
            .map(|value| value.report.reason.clone()),
        activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: Some(
            pair.activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
        ),
        changed_fields: pair.activation.metadata.field_expectations.clone(),
        backup_command_summary: Some(summaries.backup_command_summary),
        apply_command_summary: Some(summaries.apply_command_summary),
        verify_command_summary: Some(summaries.verify_command_summary),
        rollback_command_summary: Some(summaries.rollback_command_summary),
        live_verification: Some(verification),
        activation_authorized: false,
        explicit_statement:
            "verify-live-target validates the bounded live-target contract only; it does not authorize or perform real production activation"
                .to_string(),
    })
}

fn apply_live_report(config: &Config) -> Result<LiveExecuteReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                None,
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    let target = match inspect_target_contract(config, &pair) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                Some(&pair),
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    if let Err(failure) = require_source_fingerprint_match(&pair, &target) {
        return Ok(report_for_failure(
            config,
            Some(&pair),
            Some(&target),
            None,
            failure,
            config.mode,
        ));
    }
    let current_gate = match inspect_current_live_gate(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                Some(&pair),
                Some(&target),
                None,
                failure,
                config.mode,
            ))
        }
    };
    if let Err(failure) = require_live_gate(&pair, &current_gate) {
        return Ok(report_for_failure(
            config,
            Some(&pair),
            Some(&target),
            Some(&current_gate),
            failure,
            config.mode,
        ));
    }
    let backup_paths = backup_artifact_paths(config, &target)?;
    if let Err(failure) = load_backup_proof(config, &backup_paths, &pair, &target) {
        return Ok(report_for_failure(
            config,
            Some(&pair),
            Some(&target),
            Some(&current_gate),
            failure,
            config.mode,
        ));
    }
    let summaries = build_command_summaries(config)?;
    let runtime_paths = runtime_paths(&config.runtime_dir);
    prepare_runtime_dir(&runtime_paths)?;
    clear_runtime_status_artifacts(&runtime_paths)?;
    let session = planned_live_session(config, &pair, &backup_paths, &summaries);
    write_text_file(
        &runtime_paths.session_path,
        &serde_json::to_string_pretty(&session)?,
    )?;

    atomic_replace_file_from_path(
        &config.target_config_path,
        &config.activation_config_path,
        &target.current_bytes,
    )?;
    let expected_activation_fingerprint = activation_decision_packet::build_config_fingerprint(
        tiny_live_activation_execute::FINGERPRINT_SCOPE,
        &pair.activation.rendered_config,
    )?;
    let apply_verification = execute_service_action_and_verify(
        config,
        &runtime_paths,
        ServiceAction::Activation,
        &expected_activation_fingerprint.sha256,
        true,
        target.service_control_wrapper_verification.clone(),
    )?;
    if apply_verification.mismatches.is_empty() {
        return Ok(LiveExecuteReport {
            generated_at: Utc::now(),
            mode: "apply_live".to_string(),
            verdict: TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyStarted,
            reason: "bounded live-target activation apply completed and post-start verification matched the rendered activation artifact".to_string(),
            activation_config_path: config.activation_config_path.display().to_string(),
            rollback_config_path: config.rollback_config_path.display().to_string(),
            activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
            rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
            target_config_path: config.target_config_path.display().to_string(),
            target_service_name: config.target_service_name.clone(),
            service_control_command_path: config.service_control_command_path.display().to_string(),
            runtime_dir: config.runtime_dir.display().to_string(),
            backup_dir: config.backup_dir.display().to_string(),
            backup_config_path: Some(backup_paths.backup_config_path.display().to_string()),
            backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
            pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
            pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
            current_pre_activation_gate_verdict: Some(serialize_enum(&current_gate.report.verdict)),
            current_pre_activation_gate_reason: Some(current_gate.report.reason.clone()),
            activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
            activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
            current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
            current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
            rendered_source_fingerprint_sha256: Some(
                pair.activation
                    .metadata
                    .source_config_fingerprint_sha256
                    .clone(),
            ),
            changed_fields: pair.activation.metadata.field_expectations.clone(),
            backup_command_summary: Some(summaries.backup_command_summary),
            apply_command_summary: Some(summaries.apply_command_summary),
            verify_command_summary: Some(summaries.verify_command_summary),
            rollback_command_summary: Some(summaries.rollback_command_summary),
            live_verification: Some(apply_verification),
            activation_authorized: false,
            explicit_statement:
                "this live-target executor contract can touch only the explicit target config and service wrapper, but this batch still does not authorize production activation on the real server"
                    .to_string(),
        });
    }

    // If the bounded restart receipt is not trustworthy, restore the paired safe rollback posture
    // immediately instead of leaving the target config half-activated.
    let rollback_verification = perform_rollback(
        config,
        &pair,
        &runtime_paths,
        target.service_control_wrapper_verification.clone(),
    )?;
    let (verdict, reason) = if rollback_verification.mismatches.is_empty() {
        (
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRolledBack,
            format!(
                "live-target activation verify failed ({}); bounded rollback restored execution.enabled=false",
                apply_verification
                    .mismatches
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "unknown activation verify failure".to_string())
            ),
        )
    } else {
        (
            TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackVerifyFailed,
            format!(
                "live-target activation verify failed and bounded rollback could not be fully verified: {}",
                rollback_verification
                    .mismatches
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "unknown rollback verify failure".to_string())
            ),
        )
    };
    let mut verification = apply_verification;
    verification.mismatches.extend(
        rollback_verification
            .mismatches
            .iter()
            .cloned()
            .map(|entry| format!("rollback: {entry}")),
    );
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "apply_live".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: Some(backup_paths.backup_config_path.display().to_string()),
        backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
        pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: Some(serialize_enum(&current_gate.report.verdict)),
        current_pre_activation_gate_reason: Some(current_gate.report.reason.clone()),
        activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: Some(
            pair.activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
        ),
        changed_fields: pair.activation.metadata.field_expectations.clone(),
        backup_command_summary: Some(summaries.backup_command_summary),
        apply_command_summary: Some(summaries.apply_command_summary),
        verify_command_summary: Some(summaries.verify_command_summary),
        rollback_command_summary: Some(summaries.rollback_command_summary),
        live_verification: Some(verification),
        activation_authorized: false,
        explicit_statement:
            "bounded live-target apply failed closed and restored the safe rollback posture; this batch still does not authorize production activation on the real server"
                .to_string(),
    })
}

fn rollback_live_report(config: &Config) -> Result<LiveExecuteReport> {
    let pair = match load_validated_artifact_pair(config) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                None,
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    let target = match inspect_target_contract(config, &pair) {
        Ok(value) => value,
        Err(failure) => {
            return Ok(report_for_failure(
                config,
                Some(&pair),
                None,
                None,
                failure,
                config.mode,
            ))
        }
    };
    let summaries = build_command_summaries(config)?;
    let runtime_paths = runtime_paths(&config.runtime_dir);
    prepare_runtime_dir(&runtime_paths)?;
    clear_runtime_status_artifacts(&runtime_paths)?;
    let backup_paths = backup_artifact_paths(config, &target)?;
    let session = planned_live_session(config, &pair, &backup_paths, &summaries);
    write_text_file(
        &runtime_paths.session_path,
        &serde_json::to_string_pretty(&session)?,
    )?;
    let verification = perform_rollback(
        config,
        &pair,
        &runtime_paths,
        target.service_control_wrapper_verification.clone(),
    )?;
    let verdict = if verification.mismatches.is_empty() {
        TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackCompleted
    } else {
        TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackVerifyFailed
    };
    let reason = if verification.mismatches.is_empty() {
        "bounded live-target rollback completed and left execution.enabled=false under the explicit target config".to_string()
    } else {
        verification
            .mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "bounded live-target rollback verification failed".to_string())
    };
    Ok(LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "rollback_live".to_string(),
        verdict,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: Some(pair.activation.metadata_path.display().to_string()),
        rollback_metadata_path: Some(pair.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: Some(backup_paths.backup_config_path.display().to_string()),
        backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
        pre_activation_gate_verdict_used: Some(pair.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: Some(pair.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        activation_plan_verdict_used: Some(pair.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: Some(pair.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: Some(target.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: Some(target.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: Some(
            pair.activation
                .metadata
                .source_config_fingerprint_sha256
                .clone(),
        ),
        changed_fields: pair.activation.metadata.field_expectations.clone(),
        backup_command_summary: Some(summaries.backup_command_summary),
        apply_command_summary: Some(summaries.apply_command_summary),
        verify_command_summary: Some(summaries.verify_command_summary),
        rollback_command_summary: Some(summaries.rollback_command_summary),
        live_verification: Some(verification),
        activation_authorized: false,
        explicit_statement:
            "bounded live-target rollback only restores the explicit safe posture; it does not authorize production activation or perform any trading action"
                .to_string(),
    })
}

fn perform_rollback(
    config: &Config,
    pair: &ValidatedArtifactPair,
    runtime_paths: &LiveRuntimePaths,
    service_control_wrapper_verification: ServiceControlWrapperVerificationSummary,
) -> Result<LiveTargetVerificationSummary> {
    let current_bytes = fs::read(&config.target_config_path).unwrap_or_default();
    atomic_replace_file_from_path(
        &config.target_config_path,
        &config.rollback_config_path,
        &current_bytes,
    )?;
    let rollback_fingerprint = activation_decision_packet::build_config_fingerprint(
        tiny_live_activation_execute::FINGERPRINT_SCOPE,
        &pair.rollback.rendered_config,
    )?;
    execute_service_action_and_verify(
        config,
        runtime_paths,
        ServiceAction::Rollback,
        &rollback_fingerprint.sha256,
        false,
        service_control_wrapper_verification,
    )
}

fn load_validated_artifact_pair(
    config: &Config,
) -> std::result::Result<ValidatedArtifactPair, ContractFailure> {
    load_validated_artifact_pair_from_paths(
        &config.activation_config_path,
        &config.rollback_config_path,
    )
}

fn load_validated_artifact_pair_from_paths(
    activation_config_path: &Path,
    rollback_config_path: &Path,
) -> std::result::Result<ValidatedArtifactPair, ContractFailure> {
    let activation =
        tiny_live_activation_execute::inspect_rendered_config_artifact(activation_config_path)
            .map_err(|error| {
                ContractFailure::new(
                    ContractFailureKind::InvalidArtifact,
                    format!(
                        "activation rendered artifact {} is invalid: {error:#}",
                        activation_config_path.display()
                    ),
                )
            })?;
    let rollback =
        tiny_live_activation_execute::inspect_rendered_config_artifact(rollback_config_path)
            .map_err(|error| {
                ContractFailure::new(
                    ContractFailureKind::InvalidArtifact,
                    format!(
                        "rollback rendered artifact {} is invalid: {error:#}",
                        rollback_config_path.display()
                    ),
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
) -> std::result::Result<(), ContractFailure> {
    if activation.metadata.render_kind != tiny_live_activation_execute::RenderKind::Activation {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation artifact must have render_kind=activation",
        ));
    }
    if rollback.metadata.render_kind != tiny_live_activation_execute::RenderKind::Rollback {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "rollback artifact must have render_kind=rollback",
        ));
    }
    if !activation.rendered_config.execution.enabled {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation artifact must keep execution.enabled=true",
        ));
    }
    if rollback.rendered_config.execution.enabled {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "rollback artifact must keep execution.enabled=false",
        ));
    }
    if activation.metadata.source_config_fingerprint_sha256
        != rollback.metadata.source_config_fingerprint_sha256
    {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation and rollback artifacts must share the same source config fingerprint",
        ));
    }
    if activation.metadata.source_config_fingerprint_scope
        != rollback.metadata.source_config_fingerprint_scope
    {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation and rollback artifacts must share the same source config fingerprint scope",
        ));
    }
    if activation.metadata.input_config_path != rollback.metadata.input_config_path {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation and rollback artifacts must point back to the same source config path",
        ));
    }
    Ok(())
}

fn validate_live_plan_contract(
    pair: &ValidatedArtifactPair,
) -> std::result::Result<(), ContractFailure> {
    if pair.activation.metadata.activation_plan_verdict
        != "activation_plan_ready_when_stage_gate_allows"
    {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            format!(
                "activation artifact does not carry a ready bounded activation-plan verdict: {}",
                pair.activation.metadata.activation_plan_verdict
            ),
        ));
    }
    if !pair.activation.metadata.activation_overlay_complete {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation artifact metadata must keep activation_overlay_complete=true",
        ));
    }
    if !pair.activation.metadata.rollback_plan_complete
        || !pair.rollback.metadata.rollback_plan_complete
    {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation/rollback artifacts must both keep rollback_plan_complete=true",
        ));
    }
    if !pair.activation.metadata.service_restart_contract_complete
        || !pair.rollback.metadata.service_restart_contract_complete
    {
        return Err(ContractFailure::new(
            ContractFailureKind::InvalidArtifact,
            "activation/rollback artifacts must both keep service_restart_contract_complete=true",
        ));
    }
    Ok(())
}

fn inspect_target_contract(
    config: &Config,
    pair: &ValidatedArtifactPair,
) -> std::result::Result<TargetContract, ContractFailure> {
    validate_target_service_name(&config.target_service_name)?;
    validate_existing_file_path(
        &config.target_config_path,
        "target live config",
        PathKind::File,
        true,
    )?;
    validate_existing_file_path(
        &config.service_control_command_path,
        "service control command",
        PathKind::File,
        true,
    )?;
    validate_directory_path(&config.runtime_dir, "live runtime dir")?;
    validate_directory_path(&config.backup_dir, "live backup dir")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&config.service_control_command_path)
            .map_err(|error| {
                ContractFailure::new(
                    ContractFailureKind::UnsafeTarget,
                    format!(
                        "failed reading service control command metadata {}: {}",
                        config.service_control_command_path.display(),
                        error
                    ),
                )
            })?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err(ContractFailure::new(
                ContractFailureKind::UnsafeTarget,
                format!(
                    "service control command {} is not executable",
                    config.service_control_command_path.display()
                ),
            ));
        }
    }
    let service_control_wrapper_verification =
        verify_service_control_wrapper_contract(&config.service_control_command_path)?;
    if !target_path_matches_artifacts(config, pair) {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "rendered artifacts were generated for {}, not for explicit target {}",
                pair.activation.metadata.input_config_path,
                config.target_config_path.display()
            ),
        ));
    }
    let current_bytes = fs::read(&config.target_config_path).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "failed reading target live config {}: {}",
                config.target_config_path.display(),
                error
            ),
        )
    })?;
    let loaded_config = load_from_path(&config.target_config_path).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "failed parsing target live config {}: {error:#}",
                config.target_config_path.display()
            ),
        )
    })?;
    let current_fingerprint = activation_decision_packet::build_config_fingerprint(
        tiny_live_activation_execute::FINGERPRINT_SCOPE,
        &loaded_config,
    )
    .map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "failed building current live config fingerprint for {}: {error:#}",
                config.target_config_path.display()
            ),
        )
    })?;
    Ok(TargetContract {
        current_bytes,
        current_fingerprint,
        service_control_wrapper_verification,
    })
}

fn verify_service_control_wrapper_contract(
    path: &Path,
) -> std::result::Result<ServiceControlWrapperVerificationSummary, ContractFailure> {
    let summary = live_service_control_wrapper_contract::verify_wrapper(path).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "service control command {} does not match the repo-managed wrapper contract: {error:#}",
                path.display()
            ),
        )
    })?;
    if !summary.mismatches.is_empty() {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "service control command {} does not match the repo-managed wrapper contract: {}",
                path.display(),
                summary.mismatches.join("; ")
            ),
        ));
    }
    Ok(summary)
}

#[allow(dead_code)]
pub(crate) fn inspect_service_control_wrapper_contract_for_live_target(
    path: &Path,
) -> Result<ServiceControlWrapperVerificationSummary> {
    verify_service_control_wrapper_contract(path).map_err(|failure| anyhow!(failure.reason))
}

#[allow(dead_code)]
pub(crate) fn inspect_target_contract_for_watch(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
) -> Result<TargetContract> {
    let pair =
        load_validated_artifact_pair_from_paths(activation_config_path, rollback_config_path)
            .map_err(|failure| anyhow!(failure.reason))?;
    let config = Config {
        mode: Mode::VerifyLiveTarget,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
        rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
    };
    inspect_target_contract(&config, &pair).map_err(|failure| anyhow!(failure.reason))
}

fn inspect_current_live_gate(
    config: &Config,
) -> std::result::Result<CurrentGateTruth, ContractFailure> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            ContractFailure::new(
                ContractFailureKind::PreActivationGate,
                format!(
                    "failed building runtime for current pre-activation gate evaluation: {error:#}"
                ),
            )
        })?;
    let report = runtime
        .block_on(
            pre_activation_gate_report::evaluate_pre_activation_gate_report(
                &config.target_config_path,
                Utc::now(),
                DEFAULT_HISTORY_CAPTURE_LIMIT,
                None,
                pre_activation_gate_report::DEFAULT_REHEARSAL_HISTORY_LIMIT,
                pre_activation_gate_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
                pre_activation_gate_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
            ),
        )
        .map_err(|error| {
            ContractFailure::new(
                ContractFailureKind::PreActivationGate,
                format!(
                    "failed evaluating current pre-activation gate for {}: {error:#}",
                    config.target_config_path.display()
                ),
            )
        })?;
    Ok(CurrentGateTruth { report })
}

fn require_source_fingerprint_match(
    pair: &ValidatedArtifactPair,
    target: &TargetContract,
) -> std::result::Result<(), ContractFailure> {
    if target.current_fingerprint.sha256
        != pair.activation.metadata.source_config_fingerprint_sha256
    {
        return Err(ContractFailure::new(
            ContractFailureKind::ConfigDrift,
            format!(
                "live target config fingerprint drift detected: current {} but rendered artifacts expect {}",
                target.current_fingerprint.sha256,
                pair.activation.metadata.source_config_fingerprint_sha256
            ),
        ));
    }
    Ok(())
}

fn require_live_gate(
    pair: &ValidatedArtifactPair,
    current_gate: &CurrentGateTruth,
) -> std::result::Result<(), ContractFailure> {
    if current_gate.report.verdict
        == pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage3
    {
        return Err(ContractFailure::new(
            ContractFailureKind::Stage3,
            format!(
                "current Stage 3 remains the hard gate for live-target activation: {}",
                current_gate.report.reason
            ),
        ));
    }
    if current_gate.report.verdict
        != pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen
    {
        return Err(ContractFailure::new(
            ContractFailureKind::PreActivationGate,
            format!(
                "current pre-activation gate remains non-green for live-target activation: {}",
                current_gate.report.reason
            ),
        ));
    }
    validate_live_plan_contract(pair)
}

fn report_for_failure(
    config: &Config,
    pair: Option<&ValidatedArtifactPair>,
    target: Option<&TargetContract>,
    current_gate: Option<&CurrentGateTruth>,
    failure: ContractFailure,
    mode: Mode,
) -> LiveExecuteReport {
    LiveExecuteReport {
        generated_at: Utc::now(),
        mode: mode_name(mode).to_string(),
        verdict: match failure.kind {
            ContractFailureKind::Stage3 => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
            }
            ContractFailureKind::PreActivationGate => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate
            }
            ContractFailureKind::ConfigDrift => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByConfigDrift
            }
            ContractFailureKind::MissingBackup => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByMissingBackup
            }
            ContractFailureKind::UnsafeTarget => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByUnsafeTarget
            }
            ContractFailureKind::InvalidArtifact => {
                TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByInvalidArtifact
            }
        },
        reason: failure.reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: pair.map(|value| value.activation.metadata_path.display().to_string()),
        rollback_metadata_path: pair.map(|value| value.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: None,
        backup_metadata_path: None,
        pre_activation_gate_verdict_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: current_gate
            .map(|value| serialize_enum(&value.report.verdict)),
        current_pre_activation_gate_reason: current_gate
            .map(|value| value.report.reason.clone()),
        activation_plan_verdict_used: pair
            .map(|value| value.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: pair
            .map(|value| value.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: target.map(|value| value.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: target
            .map(|value| value.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: pair
            .map(|value| value.activation.metadata.source_config_fingerprint_sha256.clone()),
        changed_fields: pair
            .map(|value| value.activation.metadata.field_expectations.clone())
            .unwrap_or_default(),
        backup_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.backup_command_summary),
        apply_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.apply_command_summary),
        verify_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.verify_command_summary),
        rollback_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.rollback_command_summary),
        live_verification: None,
        activation_authorized: false,
        explicit_statement:
            "this live-target executor path remains non-authorizing in this batch; it must not be treated as permission to enable production execution on the real server"
                .to_string(),
    }
}

fn report_for_verify_invalid(
    config: &Config,
    pair: Option<&ValidatedArtifactPair>,
    target: Option<&TargetContract>,
    current_gate: Option<&CurrentGateTruth>,
    reason: String,
) -> LiveExecuteReport {
    LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "verify_live_target".to_string(),
        verdict: TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyInvalid,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: pair.map(|value| value.activation.metadata_path.display().to_string()),
        rollback_metadata_path: pair.map(|value| value.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: None,
        backup_metadata_path: None,
        pre_activation_gate_verdict_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: current_gate
            .map(|value| serialize_enum(&value.report.verdict)),
        current_pre_activation_gate_reason: current_gate
            .map(|value| value.report.reason.clone()),
        activation_plan_verdict_used: pair
            .map(|value| value.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: pair
            .map(|value| value.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: target.map(|value| value.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: target
            .map(|value| value.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: pair
            .map(|value| value.activation.metadata.source_config_fingerprint_sha256.clone()),
        changed_fields: pair
            .map(|value| value.activation.metadata.field_expectations.clone())
            .unwrap_or_default(),
        backup_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.backup_command_summary),
        apply_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.apply_command_summary),
        verify_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.verify_command_summary),
        rollback_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.rollback_command_summary),
        live_verification: None,
        activation_authorized: false,
        explicit_statement:
            "verify-live-target only checks the bounded contract and still does not authorize production activation by itself"
                .to_string(),
    }
}

fn report_for_backup_failure(
    config: &Config,
    pair: Option<&ValidatedArtifactPair>,
    target: Option<&TargetContract>,
    reason: String,
) -> LiveExecuteReport {
    LiveExecuteReport {
        generated_at: Utc::now(),
        mode: "backup_current_config".to_string(),
        verdict: TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupRefused,
        reason,
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: pair.map(|value| value.activation.metadata_path.display().to_string()),
        rollback_metadata_path: pair.map(|value| value.rollback.metadata_path.display().to_string()),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_config_path: None,
        backup_metadata_path: None,
        pre_activation_gate_verdict_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason_used: pair
            .map(|value| value.activation.metadata.pre_activation_gate_reason.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        activation_plan_verdict_used: pair
            .map(|value| value.activation.metadata.activation_plan_verdict.clone()),
        activation_plan_reason_used: pair
            .map(|value| value.activation.metadata.activation_plan_reason.clone()),
        current_target_fingerprint_scope: target.map(|value| value.current_fingerprint.scope.clone()),
        current_target_fingerprint_sha256: target
            .map(|value| value.current_fingerprint.sha256.clone()),
        rendered_source_fingerprint_sha256: pair
            .map(|value| value.activation.metadata.source_config_fingerprint_sha256.clone()),
        changed_fields: pair
            .map(|value| value.activation.metadata.field_expectations.clone())
            .unwrap_or_default(),
        backup_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.backup_command_summary),
        apply_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.apply_command_summary),
        verify_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.verify_command_summary),
        rollback_command_summary: build_command_summaries(config)
            .ok()
            .map(|value| value.rollback_command_summary),
        live_verification: None,
        activation_authorized: false,
        explicit_statement:
            "backup creation was refused before any live-target mutation; this batch still does not authorize production activation"
                .to_string(),
    }
}

fn build_command_summaries(config: &Config) -> Result<CommandSummaries> {
    let exe = env::current_exe()
        .with_context(|| "failed resolving current executable for live-target executor")?;
    let common = format!(
        "{} --activation-config {} --rollback-config {} --target-config {} --target-service {} --service-control-command {} --runtime-dir {} --backup-dir {}",
        exe.display(),
        config.activation_config_path.display(),
        config.rollback_config_path.display(),
        config.target_config_path.display(),
        config.target_service_name,
        config.service_control_command_path.display(),
        config.runtime_dir.display(),
        config.backup_dir.display()
    );
    Ok(CommandSummaries {
        backup_command_summary: format!("{common} --backup-current-config"),
        apply_command_summary: format!("{common} --apply-live"),
        verify_command_summary: format!("{common} --verify-live-target"),
        rollback_command_summary: format!("{common} --rollback-live"),
    })
}

fn planned_live_session(
    config: &Config,
    pair: &ValidatedArtifactPair,
    backup_paths: &BackupPaths,
    summaries: &CommandSummaries,
) -> LiveSession {
    LiveSession {
        session_version: SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        activation_config_path: config.activation_config_path.display().to_string(),
        rollback_config_path: config.rollback_config_path.display().to_string(),
        activation_metadata_path: pair.activation.metadata_path.display().to_string(),
        rollback_metadata_path: pair.rollback.metadata_path.display().to_string(),
        target_config_path: config.target_config_path.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        service_control_command_path: config.service_control_command_path.display().to_string(),
        runtime_dir: config.runtime_dir.display().to_string(),
        backup_dir: config.backup_dir.display().to_string(),
        backup_metadata_path: Some(backup_paths.backup_metadata_path.display().to_string()),
        source_config_fingerprint_sha256: pair
            .activation
            .metadata
            .source_config_fingerprint_sha256
            .clone(),
        source_config_fingerprint_scope: pair
            .activation
            .metadata
            .source_config_fingerprint_scope
            .clone(),
        startup_timeout_ms: config.startup_timeout_ms,
        rollback_timeout_ms: config.rollback_timeout_ms,
        apply_command_summary: summaries.apply_command_summary.clone(),
        verify_command_summary: summaries.verify_command_summary.clone(),
        rollback_command_summary: summaries.rollback_command_summary.clone(),
        explicit_statement:
            "live-target session metadata records bounded activation/rollback intent only; it does not authorize production activation by itself"
                .to_string(),
    }
}

fn backup_artifact_paths(config: &Config, target: &TargetContract) -> Result<BackupPaths> {
    let config_name = sanitize_file_component(
        config
            .target_config_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("live.server.toml"),
    );
    let service_name = sanitize_file_component(&config.target_service_name);
    let base = format!(
        "tiny_live_live_backup__{}__{}__{}",
        service_name, config_name, target.current_fingerprint.sha256
    );
    Ok(BackupPaths {
        backup_config_path: config.backup_dir.join(format!("{base}.toml")),
        backup_metadata_path: config.backup_dir.join(format!("{base}.metadata.json")),
    })
}

#[allow(dead_code)]
pub(crate) fn backup_artifact_paths_for_watch(
    target_config_path: &Path,
    target_service_name: &str,
    backup_dir: &Path,
    target_fingerprint_sha256: &str,
) -> (PathBuf, PathBuf) {
    let config_name = sanitize_file_component(
        target_config_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("live.server.toml"),
    );
    let service_name = sanitize_file_component(target_service_name);
    let base = format!(
        "tiny_live_live_backup__{}__{}__{}",
        service_name, config_name, target_fingerprint_sha256
    );
    (
        backup_dir.join(format!("{base}.toml")),
        backup_dir.join(format!("{base}.metadata.json")),
    )
}

fn load_backup_proof(
    config: &Config,
    backup_paths: &BackupPaths,
    pair: &ValidatedArtifactPair,
    target: &TargetContract,
) -> std::result::Result<LiveBackupMetadata, ContractFailure> {
    if !backup_paths.backup_metadata_path.exists() || !backup_paths.backup_config_path.exists() {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "matching live-target backup proof is missing under {}; run --backup-current-config first",
                config.backup_dir.display()
            ),
        ));
    }
    let raw = fs::read_to_string(&backup_paths.backup_metadata_path).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "failed reading live-target backup metadata {}: {}",
                backup_paths.backup_metadata_path.display(),
                error
            ),
        )
    })?;
    let metadata: LiveBackupMetadata = serde_json::from_str(&raw).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "failed parsing live-target backup metadata {}: {}",
                backup_paths.backup_metadata_path.display(),
                error
            ),
        )
    })?;
    if metadata.metadata_version != BACKUP_METADATA_VERSION {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "unsupported live-target backup metadata version {}",
                metadata.metadata_version
            ),
        ));
    }
    if metadata.target_config_path != config.target_config_path.display().to_string() {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            "backup metadata target_config_path does not match the explicit live target"
                .to_string(),
        ));
    }
    if metadata.target_service_name != config.target_service_name {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            "backup metadata target_service_name does not match the explicit live target"
                .to_string(),
        ));
    }
    if metadata.source_config_fingerprint_sha256 != target.current_fingerprint.sha256 {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "backup metadata fingerprint {} does not match the current live target fingerprint {}",
                metadata.source_config_fingerprint_sha256, target.current_fingerprint.sha256
            ),
        ));
    }
    if metadata.activation_config_path != config.activation_config_path.display().to_string()
        || metadata.rollback_config_path != config.rollback_config_path.display().to_string()
    {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            "backup metadata does not point back to the same activation/rollback artifact pair"
                .to_string(),
        ));
    }
    if metadata.activation_metadata_path != pair.activation.metadata_path.display().to_string()
        || metadata.rollback_metadata_path != pair.rollback.metadata_path.display().to_string()
    {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            "backup metadata does not point back to the same activation/rollback metadata pair"
                .to_string(),
        ));
    }
    let backup_bytes = fs::read(&backup_paths.backup_config_path).map_err(|error| {
        ContractFailure::new(
            ContractFailureKind::MissingBackup,
            format!(
                "failed reading live-target backup {}: {}",
                backup_paths.backup_config_path.display(),
                error
            ),
        )
    })?;
    if sha256_hex(&backup_bytes) != metadata.backed_up_config_sha256 {
        return Err(ContractFailure::new(
            ContractFailureKind::MissingBackup,
            "backup config sha256 does not match backup metadata".to_string(),
        ));
    }
    Ok(metadata)
}

#[allow(dead_code)]
pub(crate) fn load_backup_proof_for_watch(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
) -> Result<LiveBackupMetadata> {
    let pair =
        load_validated_artifact_pair_from_paths(activation_config_path, rollback_config_path)
            .map_err(|failure| anyhow!(failure.reason))?;
    validate_target_service_name(target_service_name).map_err(|failure| anyhow!(failure.reason))?;
    validate_existing_file_path(
        target_config_path,
        "target live config",
        PathKind::File,
        true,
    )
    .map_err(|failure| anyhow!(failure.reason))?;
    validate_existing_file_path(
        service_control_command_path,
        "service control command",
        PathKind::File,
        true,
    )
    .map_err(|failure| anyhow!(failure.reason))?;
    validate_directory_path(runtime_dir, "live runtime dir")
        .map_err(|failure| anyhow!(failure.reason))?;
    validate_directory_path(backup_dir, "live backup dir")
        .map_err(|failure| anyhow!(failure.reason))?;
    let (backup_config_path, backup_metadata_path) = backup_artifact_paths_for_watch(
        target_config_path,
        target_service_name,
        backup_dir,
        &pair.activation.metadata.source_config_fingerprint_sha256,
    );
    if !backup_metadata_path.exists() || !backup_config_path.exists() {
        bail!(
            "matching live-target backup proof is missing under {}; run --backup-current-config before activation apply",
            backup_dir.display()
        );
    }
    let raw = fs::read_to_string(&backup_metadata_path).with_context(|| {
        format!(
            "failed reading live-target backup metadata {}",
            backup_metadata_path.display()
        )
    })?;
    let metadata: LiveBackupMetadata = serde_json::from_str(&raw).with_context(|| {
        format!(
            "failed parsing live-target backup metadata {}",
            backup_metadata_path.display()
        )
    })?;
    if metadata.metadata_version != BACKUP_METADATA_VERSION {
        bail!(
            "unsupported live-target backup metadata version {}",
            metadata.metadata_version
        );
    }
    if metadata.target_config_path != target_config_path.display().to_string() {
        bail!("backup metadata target_config_path does not match the explicit live target");
    }
    if metadata.target_service_name != target_service_name {
        bail!("backup metadata target_service_name does not match the explicit live target");
    }
    if metadata.source_config_fingerprint_sha256
        != pair.activation.metadata.source_config_fingerprint_sha256
    {
        bail!(
            "backup metadata source fingerprint {} does not match rendered artifact source fingerprint {}",
            metadata.source_config_fingerprint_sha256,
            pair.activation.metadata.source_config_fingerprint_sha256
        );
    }
    if metadata.activation_config_path != activation_config_path.display().to_string()
        || metadata.rollback_config_path != rollback_config_path.display().to_string()
    {
        bail!("backup metadata does not point back to the same activation/rollback artifact pair");
    }
    if metadata.activation_metadata_path != pair.activation.metadata_path.display().to_string()
        || metadata.rollback_metadata_path != pair.rollback.metadata_path.display().to_string()
    {
        bail!("backup metadata does not point back to the same activation/rollback metadata pair");
    }
    let backup_bytes = fs::read(&backup_config_path).with_context(|| {
        format!(
            "failed reading live-target backup {}",
            backup_config_path.display()
        )
    })?;
    if sha256_hex(&backup_bytes) != metadata.backed_up_config_sha256 {
        bail!("backup config sha256 does not match backup metadata");
    }
    Ok(metadata)
}

#[allow(dead_code)]
pub(crate) fn load_validated_artifact_pair_for_live_target(
    activation_config_path: &Path,
    rollback_config_path: &Path,
) -> Result<(
    tiny_live_activation_execute::LoadedRenderedConfigArtifact,
    tiny_live_activation_execute::LoadedRenderedConfigArtifact,
)> {
    let pair =
        load_validated_artifact_pair_from_paths(activation_config_path, rollback_config_path)
            .map_err(|failure| anyhow!(failure.reason))?;
    Ok((pair.activation, pair.rollback))
}

#[allow(dead_code)]
pub(crate) fn validate_target_service_name_for_live_target(
    target_service_name: &str,
) -> Result<()> {
    validate_target_service_name(target_service_name).map_err(|failure| anyhow!(failure.reason))
}

pub(crate) fn runtime_paths(runtime_dir: &Path) -> LiveRuntimePaths {
    LiveRuntimePaths {
        runtime_dir: runtime_dir.to_path_buf(),
        session_path: runtime_dir.join("tiny_live_live_execute.session.json"),
        apply_status_path: runtime_dir.join("tiny_live_live_execute.activation.status.json"),
        rollback_status_path: runtime_dir.join("tiny_live_live_execute.rollback.status.json"),
        log_path: runtime_dir.join("tiny_live_live_execute.log"),
    }
}

fn prepare_runtime_dir(paths: &LiveRuntimePaths) -> Result<()> {
    fs::create_dir_all(&paths.runtime_dir).with_context(|| {
        format!(
            "failed creating live-target runtime dir {}",
            paths.runtime_dir.display()
        )
    })
}

fn clear_runtime_status_artifacts(paths: &LiveRuntimePaths) -> Result<()> {
    for path in [
        &paths.apply_status_path,
        &paths.rollback_status_path,
        &paths.session_path,
    ] {
        if path.exists() {
            fs::remove_file(path)
                .with_context(|| format!("failed clearing runtime artifact {}", path.display()))?;
        }
    }
    Ok(())
}

fn execute_service_action_and_verify(
    config: &Config,
    runtime_paths: &LiveRuntimePaths,
    action: ServiceAction,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
    service_control_wrapper_verification: ServiceControlWrapperVerificationSummary,
) -> Result<LiveTargetVerificationSummary> {
    let status_path = match action {
        ServiceAction::Activation => &runtime_paths.apply_status_path,
        ServiceAction::Rollback => &runtime_paths.rollback_status_path,
    };
    if status_path.exists() {
        fs::remove_file(status_path).ok();
    }
    let command_exit_success = run_service_control_command(
        config,
        action,
        status_path,
        expected_fingerprint_sha256,
        expected_execution_enabled,
    )?;
    let status_artifact_present = status_path.exists();
    let mut mismatches = Vec::new();
    let mut status_restart_successful = None;
    let mut observed_execution_enabled = None;
    let mut observed_config_fingerprint_sha256 = None;
    if !command_exit_success {
        mismatches.push(format!(
            "service control command failed for {}",
            action.as_str()
        ));
    }
    let current_target = load_from_path(&config.target_config_path)?;
    let current_fingerprint = activation_decision_packet::build_config_fingerprint(
        tiny_live_activation_execute::FINGERPRINT_SCOPE,
        &current_target,
    )?;
    if current_fingerprint.sha256 != expected_fingerprint_sha256 {
        mismatches.push(format!(
            "target config fingerprint after {} is {} but expected {}",
            action.as_str(),
            current_fingerprint.sha256,
            expected_fingerprint_sha256
        ));
    }
    if current_target.execution.enabled != expected_execution_enabled {
        mismatches.push(format!(
            "target config execution.enabled after {} is {} but expected {}",
            action.as_str(),
            current_target.execution.enabled,
            expected_execution_enabled
        ));
    }
    if status_artifact_present {
        let status = load_live_service_status(status_path)?;
        status_restart_successful = Some(status.restart_successful);
        observed_execution_enabled = Some(status.observed_execution_enabled);
        observed_config_fingerprint_sha256 =
            Some(status.observed_config_fingerprint_sha256.clone());
        mismatches.extend(verify_service_status(
            &status,
            action,
            config,
            expected_fingerprint_sha256,
            expected_execution_enabled,
        ));
    } else {
        mismatches.push(format!(
            "service status artifact {} is missing after {}",
            status_path.display(),
            action.as_str()
        ));
    }
    Ok(LiveTargetVerificationSummary {
        target_config_path_matches_rendered_source: true,
        current_target_fingerprint_matches_rendered_source: Some(
            current_fingerprint.sha256 == expected_fingerprint_sha256,
        ),
        backup_proof_present: None,
        status_artifact_present,
        status_restart_successful,
        observed_execution_enabled,
        observed_config_fingerprint_sha256,
        service_control_wrapper_verification: Some(service_control_wrapper_verification),
        mismatches,
    })
}

fn run_service_control_command(
    config: &Config,
    action: ServiceAction,
    status_path: &Path,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
) -> Result<bool> {
    if let Some(parent) = status_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = runtime_paths(&config.runtime_dir).log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime_paths(&config.runtime_dir).log_path)
        .with_context(|| {
            format!(
                "failed opening live-target log {}",
                runtime_paths(&config.runtime_dir).log_path.display()
            )
        })?;
    let stderr = log
        .try_clone()
        .with_context(|| "failed cloning live-target log handle")?;
    let mut child = Command::new(&config.service_control_command_path)
        .arg("--action")
        .arg(action.as_str())
        .arg("--service-name")
        .arg(&config.target_service_name)
        .arg("--target-config")
        .arg(&config.target_config_path)
        .arg("--runtime-dir")
        .arg(&config.runtime_dir)
        .arg("--status-path")
        .arg(status_path)
        .arg("--expected-config-fingerprint")
        .arg(expected_fingerprint_sha256)
        .arg("--expected-execution-enabled")
        .arg(expected_execution_enabled.to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| {
            format!(
                "failed spawning service control command {} for action {}",
                config.service_control_command_path.display(),
                action.as_str()
            )
        })?;
    wait_for_child(
        &mut child,
        match action {
            ServiceAction::Activation => config.startup_timeout_ms,
            ServiceAction::Rollback => config.rollback_timeout_ms,
        },
    )
}

fn wait_for_child(child: &mut Child, timeout_ms: u64) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status.success());
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

pub(crate) fn load_live_service_status(path: &Path) -> Result<LiveServiceStatus> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading live-target status {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing live-target status {}", path.display()))
}

#[allow(dead_code)]
pub(crate) fn verify_live_service_status_for_watch(
    status: &LiveServiceStatus,
    action: &str,
    target_config_path: &Path,
    target_service_name: &str,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    if status.status_version != STATUS_VERSION {
        mismatches.push(format!(
            "service status version must be {}, found {}",
            STATUS_VERSION, status.status_version
        ));
    }
    if status.action != action {
        mismatches.push(format!(
            "service status action must be {}, found {}",
            action, status.action
        ));
    }
    if status.service_name != target_service_name {
        mismatches
            .push("service status service_name does not match the explicit target".to_string());
    }
    if status.target_config_path != target_config_path.display().to_string() {
        mismatches.push(
            "service status target_config_path does not match the explicit target".to_string(),
        );
    }
    if status.expected_config_fingerprint_sha256 != expected_fingerprint_sha256 {
        mismatches.push(
            "service status expected_config_fingerprint_sha256 does not match the bounded contract"
                .to_string(),
        );
    }
    if status.observed_config_fingerprint_sha256 != expected_fingerprint_sha256 {
        mismatches.push(
            "service status observed_config_fingerprint_sha256 does not match the expected target fingerprint"
                .to_string(),
        );
    }
    if status.expected_execution_enabled != expected_execution_enabled {
        mismatches.push(
            "service status expected_execution_enabled does not match the bounded contract"
                .to_string(),
        );
    }
    if status.observed_execution_enabled != expected_execution_enabled {
        mismatches.push(
            "service status observed_execution_enabled does not match the expected target posture"
                .to_string(),
        );
    }
    if !status.restart_successful {
        mismatches.push("service status restart_successful must be true".to_string());
    }
    mismatches
}

fn verify_service_status(
    status: &LiveServiceStatus,
    action: ServiceAction,
    config: &Config,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    if status.status_version != STATUS_VERSION {
        mismatches.push(format!(
            "service status version must be {}, found {}",
            STATUS_VERSION, status.status_version
        ));
    }
    if status.action != action.as_str() {
        mismatches.push(format!(
            "service status action must be {}, found {}",
            action.as_str(),
            status.action
        ));
    }
    if status.service_name != config.target_service_name {
        mismatches
            .push("service status service_name does not match the explicit target".to_string());
    }
    if status.target_config_path != config.target_config_path.display().to_string() {
        mismatches.push(
            "service status target_config_path does not match the explicit target".to_string(),
        );
    }
    if status.expected_config_fingerprint_sha256 != expected_fingerprint_sha256 {
        mismatches.push(
            "service status expected_config_fingerprint_sha256 does not match the bounded contract"
                .to_string(),
        );
    }
    if status.observed_config_fingerprint_sha256 != expected_fingerprint_sha256 {
        mismatches.push(
            "service status observed_config_fingerprint_sha256 does not match the expected target fingerprint"
                .to_string(),
        );
    }
    if status.expected_execution_enabled != expected_execution_enabled {
        mismatches.push(
            "service status expected_execution_enabled does not match the bounded contract"
                .to_string(),
        );
    }
    if status.observed_execution_enabled != expected_execution_enabled {
        mismatches.push(
            "service status observed_execution_enabled does not match the expected target posture"
                .to_string(),
        );
    }
    if !status.restart_successful {
        mismatches.push("service status restart_successful must be true".to_string());
    }
    mismatches
}

fn validate_target_service_name(service_name: &str) -> std::result::Result<(), ContractFailure> {
    if service_name.is_empty()
        || !service_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            "target service name must use only ASCII alnum, '.', '-', or '_'".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum PathKind {
    File,
    Directory,
}

fn validate_existing_file_path(
    path: &Path,
    label: &str,
    kind: PathKind,
    require_exists: bool,
) -> std::result::Result<(), ContractFailure> {
    if !path.is_absolute() {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!("{label} must be an absolute path"),
        ));
    }
    let anchor = find_existing_anchor(path, label, kind)?;
    if require_exists && anchor != path {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!("{label} {} must already exist", path.display()),
        ));
    }
    reject_existing_descendant_symlinks(path, &anchor, label)?;
    Ok(())
}

fn validate_directory_path(path: &Path, label: &str) -> std::result::Result<(), ContractFailure> {
    if !path.is_absolute() {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!("{label} must be an absolute path"),
        ));
    }
    if path == Path::new("/") {
        return Err(ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!("{label} cannot be /"),
        ));
    }
    let anchor = find_existing_anchor(path, label, PathKind::Directory)?;
    reject_existing_descendant_symlinks(path, &anchor, label)?;
    Ok(())
}

fn find_existing_anchor(
    path: &Path,
    label: &str,
    kind: PathKind,
) -> std::result::Result<PathBuf, ContractFailure> {
    let mut current = path.to_path_buf();
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(ContractFailure::new(
                        ContractFailureKind::UnsafeTarget,
                        format!("{label} {} must not be a symlink", current.display()),
                    ));
                }
                if current == path {
                    match kind {
                        PathKind::File if !metadata.is_file() => {
                            return Err(ContractFailure::new(
                                ContractFailureKind::UnsafeTarget,
                                format!("{label} {} is not a regular file", path.display()),
                            ))
                        }
                        PathKind::Directory if !metadata.is_dir() => {
                            return Err(ContractFailure::new(
                                ContractFailureKind::UnsafeTarget,
                                format!("{label} {} is not a directory", path.display()),
                            ))
                        }
                        _ => {}
                    }
                }
                return Ok(current);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current = current.parent().map(Path::to_path_buf).ok_or_else(|| {
                    ContractFailure::new(
                        ContractFailureKind::UnsafeTarget,
                        format!("{label} {} has no existing parent anchor", path.display()),
                    )
                })?;
            }
            Err(error) => {
                return Err(ContractFailure::new(
                    ContractFailureKind::UnsafeTarget,
                    format!(
                        "failed inspecting {label} path component {}: {}",
                        current.display(),
                        error
                    ),
                ))
            }
        }
    }
}

fn reject_existing_descendant_symlinks(
    path: &Path,
    anchor: &Path,
    label: &str,
) -> std::result::Result<(), ContractFailure> {
    let mut current = anchor.to_path_buf();
    let relative = path.strip_prefix(anchor).map_err(|_| {
        ContractFailure::new(
            ContractFailureKind::UnsafeTarget,
            format!(
                "{label} {} must stay under {}",
                path.display(),
                anchor.display()
            ),
        )
    })?;
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(ContractFailure::new(
                        ContractFailureKind::UnsafeTarget,
                        format!(
                            "{label} path component {} is a symlink; symlinked live-target paths are not allowed",
                            current.display()
                        ),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(ContractFailure::new(
                    ContractFailureKind::UnsafeTarget,
                    format!(
                        "failed inspecting {label} path component {}: {}",
                        current.display(),
                        error
                    ),
                ))
            }
        }
    }
    Ok(())
}

fn target_path_matches_artifacts(config: &Config, pair: &ValidatedArtifactPair) -> bool {
    let target = config.target_config_path.display().to_string();
    pair.activation.metadata.input_config_path == target
        && pair.rollback.metadata.input_config_path == target
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

fn atomic_replace_file_from_path(
    target_path: &Path,
    rendered_path: &Path,
    current_bytes: &[u8],
) -> Result<()> {
    let rendered_bytes = fs::read(rendered_path).with_context(|| {
        format!(
            "failed reading rendered artifact {} for atomic live-target replace",
            rendered_path.display()
        )
    })?;
    atomic_replace_bytes(target_path, &rendered_bytes, current_bytes)
}

fn atomic_replace_bytes(target_path: &Path, bytes: &[u8], current_bytes: &[u8]) -> Result<()> {
    let parent = target_path
        .parent()
        .ok_or_else(|| anyhow!("cannot derive parent dir for {}", target_path.display()))?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed creating parent dir for target config {}",
            target_path.display()
        )
    })?;
    let unique = format!(
        ".tmp-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let file_name = target_path
        .file_name()
        .ok_or_else(|| anyhow!("cannot derive file name for {}", target_path.display()))?;
    let temp_path =
        target_path.with_file_name(format!("{}{}", file_name.to_string_lossy(), unique));
    fs::write(&temp_path, bytes).with_context(|| {
        format!(
            "failed writing temporary target config {}",
            temp_path.display()
        )
    })?;
    if let Ok(metadata) = fs::metadata(target_path) {
        fs::set_permissions(&temp_path, metadata.permissions()).with_context(|| {
            format!(
                "failed preserving file permissions while replacing {}",
                target_path.display()
            )
        })?;
    }
    fs::rename(&temp_path, target_path).with_context(|| {
        format!(
            "failed replacing target config {} with temporary file {}",
            target_path.display(),
            temp_path.display()
        )
    })?;
    let replaced_bytes = fs::read(target_path)?;
    if replaced_bytes == current_bytes {
        bail!(
            "atomic replace did not change target config {}; refusing to continue",
            target_path.display()
        );
    }
    Ok(())
}

fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLive => "plan_live",
        Mode::BackupCurrentConfig => "backup_current_config",
        Mode::RenderLiveApplyScript => "render_live_apply_script",
        Mode::RenderLiveRollbackScript => "render_live_rollback_script",
        Mode::VerifyLiveTarget => "verify_live_target",
        Mode::ApplyLive => "apply_live",
        Mode::RollbackLive => "rollback_live",
    }
}

fn render_human(report: &LiveExecuteReport) -> String {
    let mut lines = vec![
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("mode={}", report.mode),
        format!("reason={}", report.reason),
        format!("activation_config_path={}", report.activation_config_path),
        format!("rollback_config_path={}", report.rollback_config_path),
        format!("target_config_path={}", report.target_config_path),
        format!("target_service_name={}", report.target_service_name),
        format!(
            "service_control_command_path={}",
            report.service_control_command_path
        ),
        format!("runtime_dir={}", report.runtime_dir),
        format!("backup_dir={}", report.backup_dir),
    ];
    if let Some(path) = &report.activation_metadata_path {
        lines.push(format!("activation_metadata_path={path}"));
    }
    if let Some(path) = &report.rollback_metadata_path {
        lines.push(format!("rollback_metadata_path={path}"));
    }
    if let Some(path) = &report.backup_config_path {
        lines.push(format!("backup_config_path={path}"));
    }
    if let Some(path) = &report.backup_metadata_path {
        lines.push(format!("backup_metadata_path={path}"));
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
    if let Some(value) = &report.current_target_fingerprint_scope {
        lines.push(format!("current_target_fingerprint_scope={value}"));
    }
    if let Some(value) = &report.current_target_fingerprint_sha256 {
        lines.push(format!("current_target_fingerprint_sha256={value}"));
    }
    if let Some(value) = &report.rendered_source_fingerprint_sha256 {
        lines.push(format!("rendered_source_fingerprint_sha256={value}"));
    }
    if !report.changed_fields.is_empty() {
        lines.push("changed_fields:".to_string());
        lines.extend(report.changed_fields.iter().map(|field| {
            format!(
                "  {}: {} -> {} ({})",
                field.field, field.source_value, field.target_value, field.reason
            )
        }));
    }
    if let Some(value) = &report.backup_command_summary {
        lines.push(format!("backup_command_summary={value}"));
    }
    if let Some(value) = &report.apply_command_summary {
        lines.push(format!("apply_command_summary={value}"));
    }
    if let Some(value) = &report.verify_command_summary {
        lines.push(format!("verify_command_summary={value}"));
    }
    if let Some(value) = &report.rollback_command_summary {
        lines.push(format!("rollback_command_summary={value}"));
    }
    if let Some(verification) = &report.live_verification {
        lines.push(format!(
            "live_target_path_matches_rendered_source={}",
            verification.target_config_path_matches_rendered_source
        ));
        if let Some(value) = verification.current_target_fingerprint_matches_rendered_source {
            lines.push(format!(
                "live_target_fingerprint_matches_rendered_source={value}"
            ));
        }
        if let Some(value) = verification.backup_proof_present {
            lines.push(format!("backup_proof_present={value}"));
        }
        lines.push(format!(
            "service_status_artifact_present={}",
            verification.status_artifact_present
        ));
        if let Some(value) = verification.status_restart_successful {
            lines.push(format!("service_status_restart_successful={value}"));
        }
        if let Some(value) = verification.observed_execution_enabled {
            lines.push(format!("service_status_observed_execution_enabled={value}"));
        }
        if let Some(value) = &verification.observed_config_fingerprint_sha256 {
            lines.push(format!(
                "service_status_observed_config_fingerprint_sha256={value}"
            ));
        }
        if let Some(wrapper) = &verification.service_control_wrapper_verification {
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
            if let Some(value) = wrapper.executable {
                lines.push(format!("service_control_wrapper_executable={value}"));
            }
            if let Some(value) = &wrapper.backend_command {
                lines.push(format!("service_control_wrapper_backend_command={value}"));
            }
            if !wrapper.supported_actions.is_empty() {
                lines.push(format!(
                    "service_control_wrapper_supported_actions={}",
                    wrapper.supported_actions.join(",")
                ));
            }
            if !wrapper.mismatches.is_empty() {
                lines.push("service_control_wrapper_mismatches:".to_string());
                lines.extend(wrapper.mismatches.iter().map(|entry| format!("  {entry}")));
            }
        }
        if !verification.mismatches.is_empty() {
            lines.push("live_verification_mismatches:".to_string());
            lines.extend(
                verification
                    .mismatches
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
pub(crate) fn build_plan_live_report_for_drill(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
) -> Result<LiveExecuteReport> {
    build_plan_live_report(&Config {
        mode: Mode::PlanLive,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms,
        rollback_timeout_ms,
    })
}

#[allow(dead_code)]
pub(crate) fn backup_current_config_report_for_cutover(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
) -> Result<LiveExecuteReport> {
    backup_current_config_report(&Config {
        mode: Mode::BackupCurrentConfig,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms,
        rollback_timeout_ms,
    })
}

#[allow(dead_code)]
pub(crate) fn verify_live_target_report_for_cutover(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
) -> Result<LiveExecuteReport> {
    verify_live_target_report(&Config {
        mode: Mode::VerifyLiveTarget,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms,
        rollback_timeout_ms,
    })
}

#[allow(dead_code)]
pub(crate) fn apply_live_report_for_cutover(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
) -> Result<LiveExecuteReport> {
    apply_live_report(&Config {
        mode: Mode::ApplyLive,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms,
        rollback_timeout_ms,
    })
}

#[allow(dead_code)]
pub(crate) fn rollback_live_report_for_cutover(
    activation_config_path: &Path,
    rollback_config_path: &Path,
    target_config_path: &Path,
    target_service_name: &str,
    service_control_command_path: &Path,
    runtime_dir: &Path,
    backup_dir: &Path,
    startup_timeout_ms: u64,
    rollback_timeout_ms: u64,
) -> Result<LiveExecuteReport> {
    rollback_live_report(&Config {
        mode: Mode::RollbackLive,
        activation_config_path: activation_config_path.to_path_buf(),
        rollback_config_path: rollback_config_path.to_path_buf(),
        target_config_path: target_config_path.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        service_control_command_path: service_control_command_path.to_path_buf(),
        runtime_dir: runtime_dir.to_path_buf(),
        backup_dir: backup_dir.to_path_buf(),
        output_path: None,
        json: false,
        startup_timeout_ms,
        rollback_timeout_ms,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn valid_rendered_artifacts_and_matching_live_config_yield_green_live_plan() {
        let fixture = live_fixture("tiny_live_live_execute_plan", GateState::Green);
        let report = build_plan_live_report(&fixture.config).expect("plan");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
        );
        assert!(report.apply_command_summary.is_some());
        assert!(report.rollback_command_summary.is_some());
        assert!(report
            .live_verification
            .as_ref()
            .and_then(|value| value.service_control_wrapper_verification.as_ref())
            .is_some());
        assert!(report
            .live_verification
            .as_ref()
            .and_then(|value| value.service_control_wrapper_verification.as_ref())
            .map(|value| value.mismatches.is_empty())
            .unwrap_or(false));
    }

    #[test]
    fn backup_artifact_is_created_deterministically_and_not_overwritten_silently() {
        let fixture = live_fixture("tiny_live_live_execute_backup", GateState::Green);
        let first = backup_current_config_report(&fixture.config).expect("first backup");
        assert_eq!(
            first.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
        );
        let second = backup_current_config_report(&fixture.config).expect("second backup");
        assert_eq!(
            second.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupRefused
        );
        assert!(second
            .reason
            .contains("refusing to overwrite existing live-target backup"));
    }

    #[test]
    fn config_drift_fingerprint_mismatch_is_refused_explicitly() {
        let fixture = live_fixture("tiny_live_live_execute_drift", GateState::Green);
        fs::write(
            &fixture.target_config_path,
            sample_drifted_safe_config_toml(),
        )
        .unwrap();
        let report = build_plan_live_report(&fixture.config).expect("plan");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByConfigDrift
        );
    }

    #[test]
    fn stage3_non_green_blocks_live_apply() {
        let fixture = live_fixture("tiny_live_live_execute_stage3", GateState::Stage3Blocked);
        backup_current_config_report(&fixture.config).unwrap();
        let report = apply_live_report(&fixture.config).expect("apply");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
        );
    }

    #[test]
    fn pre_activation_gate_non_green_blocks_live_apply() {
        let fixture = live_fixture(
            "tiny_live_live_execute_pregate",
            GateState::PreActivationBlocked,
        );
        backup_current_config_report(&fixture.config).unwrap();
        let report = apply_live_report(&fixture.config).expect("apply");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate
        );
    }

    #[test]
    fn invalid_rendered_artifact_blocks_live_apply() {
        let fixture = live_fixture("tiny_live_live_execute_invalid_artifact", GateState::Green);
        fs::write(
            tiny_live_activation_execute::metadata_path_for_rendered_config(
                &fixture.activation_config_path,
            ),
            "{invalid json",
        )
        .unwrap();
        backup_current_config_report(&fixture.config).unwrap_or_else(|_| panic!("backup"));
        let report = apply_live_report(&fixture.config).expect("apply");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByInvalidArtifact
        );
    }

    #[cfg(unix)]
    #[test]
    fn post_start_verify_failure_triggers_bounded_rollback() {
        let fixture = live_fixture("tiny_live_live_execute_apply_rollback", GateState::Green);
        backup_current_config_report(&fixture.config).unwrap();
        fs::create_dir_all(&fixture.runtime_dir).unwrap();
        fs::write(fixture.runtime_dir.join("force_apply_status_mismatch"), "1").unwrap();
        let report = apply_live_report(&fixture.config).expect("apply");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRolledBack
        );
        let final_config = load_from_path(&fixture.target_config_path).unwrap();
        assert!(!final_config.execution.enabled);
        assert!(fixture
            .runtime_dir
            .join("tiny_live_live_execute.rollback.status.json")
            .exists());
    }

    #[cfg(unix)]
    #[test]
    fn rollback_restores_execution_enabled_false() {
        let fixture = live_fixture("tiny_live_live_execute_rollback", GateState::Green);
        fs::write(&fixture.target_config_path, sample_activation_config_toml()).unwrap();
        let report = rollback_live_report(&fixture.config).expect("rollback");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackCompleted
        );
        let final_config = load_from_path(&fixture.target_config_path).unwrap();
        assert!(!final_config.execution.enabled);
    }

    #[test]
    fn verify_live_target_accepts_green_contract() {
        let fixture = live_fixture("tiny_live_live_execute_verify", GateState::Green);
        let report = verify_live_target_report(&fixture.config).expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyOk
        );
        assert!(report
            .live_verification
            .as_ref()
            .and_then(|value| value.service_control_wrapper_verification.as_ref())
            .map(|value| value.mismatches.is_empty())
            .unwrap_or(false));
        assert_eq!(
            report
                .live_verification
                .as_ref()
                .and_then(|value| value.backup_proof_present),
            Some(false)
        );
        let backup = backup_current_config_report(&fixture.config).unwrap();
        assert_eq!(
            backup.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
        );
        let verified = verify_live_target_report(&fixture.config).expect("verify");
        assert_eq!(
            verified.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyOk
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_wrapper_service_control_command_is_refused_by_live_plan_and_verify() {
        let fixture = live_fixture("tiny_live_live_execute_non_wrapper", GateState::Green);
        write_non_wrapper_service_control_command(&fixture.config.service_control_command_path);

        let plan = build_plan_live_report(&fixture.config).expect("plan");
        assert_eq!(
            plan.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByUnsafeTarget
        );
        assert!(plan.reason.contains("repo-managed wrapper contract"));

        let verify = verify_live_target_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyInvalid
        );
        assert!(verify.reason.contains("repo-managed wrapper contract"));
    }

    #[test]
    fn stale_green_rendered_artifact_is_rejected_when_current_stage3_is_blocked() {
        let fixture = live_fixture(
            "tiny_live_live_execute_verify_stage3_now_blocked",
            GateState::Stage3Blocked,
        );
        let report = verify_live_target_report(&fixture.config).expect("verify");
        assert_eq!(
            report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveVerifyInvalid
        );
        assert_eq!(
            report.current_pre_activation_gate_verdict.as_deref(),
            Some("blocked_by_stage3")
        );
        assert!(report
            .reason
            .contains("current pre-activation gate is non-green"));
    }

    #[test]
    fn live_scripts_render_public_modes_only() {
        let fixture = live_fixture("tiny_live_live_execute_scripts", GateState::Green);
        let apply_script = fixture.fixture_dir.join("apply-live.sh");
        let rollback_script = fixture.fixture_dir.join("rollback-live.sh");
        let apply_report = render_live_script_report(
            &Config {
                mode: Mode::RenderLiveApplyScript,
                output_path: Some(apply_script.clone()),
                ..fixture.config.clone()
            },
            ScriptKind::Apply,
        )
        .unwrap();
        let rollback_report = render_live_script_report(
            &Config {
                mode: Mode::RenderLiveRollbackScript,
                output_path: Some(rollback_script.clone()),
                ..fixture.config.clone()
            },
            ScriptKind::Rollback,
        )
        .unwrap();
        assert_eq!(
            apply_report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyScriptRendered
        );
        assert_eq!(
            rollback_report.verdict,
            TinyLiveLiveExecuteVerdict::TinyLiveLiveRollbackScriptRendered
        );
        let apply_contents = fs::read_to_string(&apply_script).unwrap();
        let rollback_contents = fs::read_to_string(&rollback_script).unwrap();
        assert!(apply_contents.contains("--apply-live"));
        assert!(rollback_contents.contains("--rollback-live"));
        assert!(!apply_contents.contains("/etc/solana-copy-bot/live.server.toml"));
        assert!(!rollback_contents.contains("systemctl restart"));
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct LiveFixture {
        fixture_dir: PathBuf,
        target_config_path: PathBuf,
        activation_config_path: PathBuf,
        runtime_dir: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
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
            serde_json::json!({"jsonrpc":"2.0","id":1,"result":123456u64}).to_string(),
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            8,
            serde_json::json!({
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
        write_rendered_artifact_pair(
            &target_config_path,
            &activation_config_path,
            &rollback_config_path,
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
        );
        LiveFixture {
            fixture_dir,
            target_config_path: target_config_path.clone(),
            activation_config_path: activation_config_path.clone(),
            runtime_dir: runtime_dir.clone(),
            config: Config {
                mode: Mode::PlanLive,
                activation_config_path,
                rollback_config_path,
                target_config_path,
                target_service_name: "solana-copy-bot.service".to_string(),
                service_control_command_path,
                runtime_dir,
                backup_dir,
                output_path: None,
                json: false,
                startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
                rollback_timeout_ms: DEFAULT_ROLLBACK_TIMEOUT_MS,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn write_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
    ) {
        let source = load_from_path(target_config_path).unwrap();
        let source_fingerprint = activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source,
        )
        .unwrap();
        let gate_verdict = "pre_activation_gates_green";
        let gate_reason = "green";
        write_rendered_artifact(
            activation_path,
            tiny_live_activation_execute::RenderKind::Activation,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, true),
            target_config_path,
            &source_fingerprint.sha256,
            false,
            true,
            gate_verdict,
            gate_reason,
        );
        write_rendered_artifact(
            rollback_path,
            tiny_live_activation_execute::RenderKind::Rollback,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, false),
            target_config_path,
            &source_fingerprint.sha256,
            false,
            false,
            gate_verdict,
            gate_reason,
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
        gate_verdict: &str,
        gate_reason: &str,
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
            rendered_config_sha256: hash,
            pre_activation_gate_verdict: gate_verdict.to_string(),
            pre_activation_gate_reason: gate_reason.to_string(),
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
                    now - chrono::Duration::minutes(5),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(15),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(25),
                    "validated_current",
                    "fresh_current",
                    true,
                );
            }
            GateState::Stage3Blocked => {
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(5),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(15),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(25),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
            }
        }
        if matches!(gate_state, GateState::Green) {
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - chrono::Duration::minutes(3),
                    "rehearsal_green",
                ))
                .unwrap();
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - chrono::Duration::minutes(12),
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
                audit_json: serde_json::json!({
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
                        "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                        "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
                    },
                    "active_follow_vs_current_raw": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": if exact_match { 2 } else { 1 },
                        "exact_match": exact_match,
                        "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                        "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
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
                        serde_json::json!(["wallet-gamma"])
                    } else {
                        serde_json::json!([])
                    },
                    "left_since_previous_cycle": [],
                    "stable_wallets_across_cycles": ["wallet-alpha", "wallet-beta"],
                    "unique_wallet_count_across_cycles": if exact_match { 3 } else { 2 },
                    "samples": []
                }
            })
                .to_string(),
                shadow_signal_json: serde_json::json!({
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
        let headers: Vec<String> = header_text.lines().map(|line| line.to_string()).collect();
        let content_length = headers
            .iter()
            .find_map(|line| {
                let lower = line.to_ascii_lowercase();
                lower
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body_bytes = header_bytes[header_end..].to_vec();
        while body_bytes.len() < content_length {
            let bytes = stream.read(&mut buf).expect("read request body");
            assert!(bytes > 0, "unexpected eof reading body");
            body_bytes.extend_from_slice(&buf[..bytes]);
        }
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
            .expect("write mock response");
        stream.flush().expect("flush mock response");
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

    fn sample_safe_config_toml() -> String {
        sample_activation_config_toml().replace("enabled = true", "enabled = false")
    }

    fn sample_drifted_safe_config_toml() -> String {
        sample_safe_config_toml().replace("copy_notional_sol = 0.05", "copy_notional_sol = 0.04")
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

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }
}
