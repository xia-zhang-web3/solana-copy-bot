use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use toml::map::Map as TomlMap;
use toml::Value as TomlValue;

#[allow(dead_code)]
#[path = "copybot_activation_decision_packet.rs"]
pub(crate) mod activation_decision_packet;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_plan.rs"]
pub(crate) mod tiny_live_activation_plan;

const USAGE: &str = "usage: copybot_tiny_live_activation_execute --config <path> (--plan | --render-activation-config --output <path> | --render-rollback-config --output <path> | --verify-rendered-config) [--json] [--expected-source-fingerprint <sha256>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>]";
const FINGERPRINT_SCOPE: &str = "prod_execution_policy_guardrails";
const METADATA_VERSION: &str = "1";
const METADATA_SUFFIX: &str = ".tiny_live_activation_execute.metadata.json";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for tiny-live activation execute")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    config_path: PathBuf,
    output_path: Option<PathBuf>,
    expected_source_fingerprint_sha256: Option<String>,
    json: bool,
    now: DateTime<Utc>,
    stage3_limit: usize,
    stage3_recent_horizon_seconds: Option<u64>,
    rehearsal_limit: usize,
    rehearsal_recent_horizon_seconds: u64,
    min_recent_acceptable_rehearsals: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plan,
    RenderActivationConfig,
    RenderRollbackConfig,
    VerifyRenderedConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RenderKind {
    Activation,
    Rollback,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLiveActivationExecuteVerdict {
    TinyLiveActivationPlanReady,
    TinyLiveActivationRendered,
    TinyLiveActivationRefusedByStage3,
    TinyLiveActivationRefusedByPreActivationGate,
    TinyLiveActivationRefusedByConfigDrift,
    TinyLiveActivationRefusedByPlanContract,
    TinyLiveActivationVerifyOk,
    TinyLiveActivationVerifyInvalid,
    TinyLiveRollbackRendered,
    TinyLiveRollbackRefusedByConfigDrift,
    TinyLiveRollbackRefusedByPlanContract,
    TinyLiveRollbackVerifyOk,
    TinyLiveRollbackVerifyInvalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FieldExpectation {
    field: String,
    source_value: Value,
    target_value: Value,
    reason: String,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RenderedConfigMetadata {
    metadata_version: String,
    render_kind: RenderKind,
    generated_at: DateTime<Utc>,
    input_config_path: String,
    output_config_path: String,
    source_config_fingerprint_scope: String,
    source_config_fingerprint_sha256: String,
    expected_source_fingerprint_sha256: Option<String>,
    rendered_config_sha256: String,
    pre_activation_gate_verdict: String,
    pre_activation_gate_reason: String,
    activation_plan_verdict: String,
    activation_plan_reason: String,
    activation_overlay_complete: bool,
    rollback_plan_complete: bool,
    service_restart_contract_complete: bool,
    field_expectations: Vec<FieldExpectation>,
    execution_untouched_by_batch: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct VerificationSummary {
    render_kind: RenderKind,
    metadata_path: String,
    rendered_config_sha256: String,
    file_hash_matches_metadata: bool,
    source_config_path_present: bool,
    source_config_fingerprint_matches_current: Option<bool>,
    metadata_mismatches: Vec<String>,
    field_mismatches: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExecuteReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLiveActivationExecuteVerdict,
    reason: String,
    input_config_path: String,
    output_config_path: Option<String>,
    metadata_path: Option<String>,
    render_kind: Option<RenderKind>,
    pre_activation_gate_verdict: Option<String>,
    pre_activation_gate_reason: Option<String>,
    activation_plan_verdict: Option<String>,
    activation_plan_reason: Option<String>,
    source_config_fingerprint_scope: Option<String>,
    source_config_fingerprint_sha256: Option<String>,
    expected_source_fingerprint_sha256: Option<String>,
    overlay_field_count: usize,
    applied_change_count: usize,
    changed_fields: Vec<FieldExpectation>,
    service_restart_contract_complete: Option<bool>,
    rollback_summary: Option<String>,
    verification: Option<VerificationSummary>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PlanContext {
    loaded_config: AppConfig,
    plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    source_fingerprint: activation_decision_packet::ConfigFingerprintSummary,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut expected_source_fingerprint_sha256: Option<String> = None;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit = tiny_live_activation_plan::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds =
        tiny_live_activation_plan::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        tiny_live_activation_plan::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--expected-source-fingerprint" => {
                expected_source_fingerprint_sha256 = Some(normalize_sha256_arg(
                    "--expected-source-fingerprint",
                    args.next(),
                )?)
            }
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--stage3-limit" => stage3_limit = parse_usize_arg("--stage3-limit", args.next())?,
            "--stage3-recent-horizon-seconds" => {
                stage3_recent_horizon_seconds = Some(parse_u64_arg(
                    "--stage3-recent-horizon-seconds",
                    args.next(),
                )?)
            }
            "--rehearsal-limit" => {
                rehearsal_limit = parse_usize_arg("--rehearsal-limit", args.next())?
            }
            "--rehearsal-recent-horizon-seconds" => {
                rehearsal_recent_horizon_seconds =
                    parse_u64_arg("--rehearsal-recent-horizon-seconds", args.next())?
            }
            "--min-recent-acceptable-rehearsals" => {
                min_recent_acceptable_rehearsals =
                    parse_usize_arg("--min-recent-acceptable-rehearsals", args.next())?
            }
            "--plan" => set_mode(&mut mode, Mode::Plan, "--plan")?,
            "--render-activation-config" => set_mode(
                &mut mode,
                Mode::RenderActivationConfig,
                "--render-activation-config",
            )?,
            "--render-rollback-config" => set_mode(
                &mut mode,
                Mode::RenderRollbackConfig,
                "--render-rollback-config",
            )?,
            "--verify-rendered-config" => set_mode(
                &mut mode,
                Mode::VerifyRenderedConfig,
                "--verify-rendered-config",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| {
        anyhow!(
            "select exactly one mode: --plan | --render-activation-config | --render-rollback-config | --verify-rendered-config"
        )
    })?;
    let config_path = config_path.ok_or_else(|| anyhow!("missing required --config"))?;

    match mode {
        Mode::Plan | Mode::VerifyRenderedConfig => {
            if output_path.is_some() {
                bail!("--output is only valid with render modes");
            }
        }
        Mode::RenderActivationConfig | Mode::RenderRollbackConfig => {
            if output_path.is_none() {
                bail!("--output is required with render modes");
            }
        }
    }
    if matches!(mode, Mode::Plan | Mode::VerifyRenderedConfig)
        && expected_source_fingerprint_sha256.is_some()
    {
        bail!("--expected-source-fingerprint is only valid with render modes");
    }

    Ok(Some(Config {
        mode,
        config_path,
        output_path,
        expected_source_fingerprint_sha256,
        json,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
    }))
}

async fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::Plan => build_plan_mode_report(&config).await?,
        Mode::RenderActivationConfig => {
            build_render_report(&config, RenderKind::Activation).await?
        }
        Mode::RenderRollbackConfig => build_render_report(&config, RenderKind::Rollback).await?,
        Mode::VerifyRenderedConfig => build_verify_report(&config)?,
    };
    Ok(if config.json {
        serde_json::to_string_pretty(&report)?
    } else {
        render_human(&report)
    })
}

async fn build_plan_mode_report(config: &Config) -> Result<ExecuteReport> {
    let context = evaluate_plan_context(config).await?;
    let (verdict, reason) = plan_mode_verdict_and_reason(&context.plan);
    Ok(ExecuteReport {
        generated_at: config.now,
        mode: "plan".to_string(),
        verdict,
        reason,
        input_config_path: config.config_path.display().to_string(),
        output_config_path: None,
        metadata_path: None,
        render_kind: None,
        pre_activation_gate_verdict: Some(context.plan.pre_activation_gate.verdict.clone()),
        pre_activation_gate_reason: Some(context.plan.pre_activation_gate.reason.clone()),
        activation_plan_verdict: Some(serialize_enum(&context.plan.verdict)),
        activation_plan_reason: Some(context.plan.reason.clone()),
        source_config_fingerprint_scope: Some(context.source_fingerprint.scope.clone()),
        source_config_fingerprint_sha256: Some(context.source_fingerprint.sha256.clone()),
        expected_source_fingerprint_sha256: None,
        overlay_field_count: context.plan.activation_overlay_changes.len(),
        applied_change_count: 0,
        changed_fields: build_target_field_expectations(
            &context.loaded_config,
            &context.plan,
            RenderKind::Activation,
        )?,
        service_restart_contract_complete: Some(context.plan.service_restart_contract_complete),
        rollback_summary: Some(rollback_summary(&context.plan)),
        verification: None,
        activation_authorized: false,
        explicit_statement:
            "this executor path renders and verifies bounded activation artifacts only; it does not authorize or perform production activation by itself"
                .to_string(),
    })
}

async fn build_render_report(config: &Config, render_kind: RenderKind) -> Result<ExecuteReport> {
    let context = evaluate_plan_context(config).await?;
    render_with_context(config, render_kind, context)
}

fn build_verify_report(config: &Config) -> Result<ExecuteReport> {
    let metadata_path = metadata_path_for_config(&config.config_path);
    let metadata_contents = fs::read_to_string(&metadata_path).with_context(|| {
        format!(
            "failed reading tiny-live activation metadata {}",
            metadata_path.display()
        )
    })?;
    let metadata: RenderedConfigMetadata = serde_json::from_str(&metadata_contents)
        .with_context(|| format!("failed parsing metadata {}", metadata_path.display()))?;
    if metadata.metadata_version != METADATA_VERSION {
        bail!(
            "unsupported rendered-config metadata version {} in {}",
            metadata.metadata_version,
            metadata_path.display()
        );
    }

    let rendered_bytes = fs::read(&config.config_path).with_context(|| {
        format!(
            "failed reading rendered config {}",
            config.config_path.display()
        )
    })?;
    let rendered_config_sha256 = sha256_hex(&rendered_bytes);
    let file_hash_matches_metadata = rendered_config_sha256 == metadata.rendered_config_sha256;
    let rendered_config = load_from_path(&config.config_path).with_context(|| {
        format!(
            "failed parsing rendered config {}",
            config.config_path.display()
        )
    })?;
    let field_mismatches =
        collect_field_expectation_mismatches(&rendered_config, &metadata.field_expectations)?;
    let mut metadata_mismatches = Vec::new();
    if !file_hash_matches_metadata {
        metadata_mismatches.push(format!(
            "rendered config sha256 mismatch: metadata={} actual={}",
            metadata.rendered_config_sha256, rendered_config_sha256
        ));
    }
    if metadata.field_expectations.is_empty() {
        metadata_mismatches.push("metadata field_expectations is empty".to_string());
    }
    if metadata.execution_untouched_by_batch != true {
        metadata_mismatches.push(
            "metadata.execution_untouched_by_batch must remain true for this executor".to_string(),
        );
    }
    if metadata.activation_authorized {
        metadata_mismatches
            .push("metadata.activation_authorized must remain false in this batch".to_string());
    }

    let source_config_path = PathBuf::from(&metadata.input_config_path);
    let source_config_path_present = source_config_path.exists();
    let source_config_fingerprint_matches_current = if source_config_path_present {
        let current_source = load_from_path(&source_config_path).with_context(|| {
            format!(
                "failed loading source config referenced by metadata {}",
                source_config_path.display()
            )
        })?;
        let current_fingerprint = activation_decision_packet::build_config_fingerprint(
            FINGERPRINT_SCOPE,
            &current_source,
        )?;
        Some(current_fingerprint.sha256 == metadata.source_config_fingerprint_sha256)
    } else {
        None
    };
    if metadata.source_config_fingerprint_scope != FINGERPRINT_SCOPE {
        metadata_mismatches.push(format!(
            "metadata source fingerprint scope mismatch: expected {FINGERPRINT_SCOPE}, got {}",
            metadata.source_config_fingerprint_scope
        ));
    }

    let verdict = match metadata.render_kind {
        RenderKind::Activation => {
            if metadata_mismatches.is_empty() && field_mismatches.is_empty() {
                TinyLiveActivationExecuteVerdict::TinyLiveActivationVerifyOk
            } else {
                TinyLiveActivationExecuteVerdict::TinyLiveActivationVerifyInvalid
            }
        }
        RenderKind::Rollback => {
            if metadata_mismatches.is_empty() && field_mismatches.is_empty() {
                TinyLiveActivationExecuteVerdict::TinyLiveRollbackVerifyOk
            } else {
                TinyLiveActivationExecuteVerdict::TinyLiveRollbackVerifyInvalid
            }
        }
    };
    let reason = if metadata_mismatches.is_empty() && field_mismatches.is_empty() {
        match metadata.render_kind {
            RenderKind::Activation => {
                "rendered activation config matches its bounded overlay metadata".to_string()
            }
            RenderKind::Rollback => {
                "rendered rollback config matches its bounded overlay metadata".to_string()
            }
        }
    } else {
        metadata_mismatches
            .first()
            .cloned()
            .or_else(|| field_mismatches.first().cloned())
            .unwrap_or_else(|| "rendered config failed verification".to_string())
    };
    Ok(ExecuteReport {
        generated_at: Utc::now(),
        mode: "verify_rendered_config".to_string(),
        verdict,
        reason,
        input_config_path: config.config_path.display().to_string(),
        output_config_path: Some(config.config_path.display().to_string()),
        metadata_path: Some(metadata_path.display().to_string()),
        render_kind: Some(metadata.render_kind),
        pre_activation_gate_verdict: Some(metadata.pre_activation_gate_verdict.clone()),
        pre_activation_gate_reason: Some(metadata.pre_activation_gate_reason.clone()),
        activation_plan_verdict: Some(metadata.activation_plan_verdict.clone()),
        activation_plan_reason: Some(metadata.activation_plan_reason.clone()),
        source_config_fingerprint_scope: Some(metadata.source_config_fingerprint_scope.clone()),
        source_config_fingerprint_sha256: Some(metadata.source_config_fingerprint_sha256.clone()),
        expected_source_fingerprint_sha256: metadata.expected_source_fingerprint_sha256.clone(),
        overlay_field_count: metadata.field_expectations.len(),
        applied_change_count: metadata
            .field_expectations
            .iter()
            .filter(|field| field.source_value != field.target_value)
            .count(),
        changed_fields: metadata.field_expectations.clone(),
        service_restart_contract_complete: Some(metadata.service_restart_contract_complete),
        rollback_summary: Some(match metadata.render_kind {
            RenderKind::Activation => {
                "rollback remains a separately rendered safe config artifact".to_string()
            }
            RenderKind::Rollback => {
                "rendered rollback config must guarantee execution.enabled=false and the current bounded safe posture"
                    .to_string()
            }
        }),
        verification: Some(VerificationSummary {
            render_kind: metadata.render_kind,
            metadata_path: metadata_path.display().to_string(),
            rendered_config_sha256,
            file_hash_matches_metadata,
            source_config_path_present,
            source_config_fingerprint_matches_current,
            metadata_mismatches,
            field_mismatches,
        }),
        activation_authorized: false,
        explicit_statement:
            "verification checks rendered-config integrity and bounded overlay truth only; it does not authorize production activation by itself"
                .to_string(),
    })
}

async fn evaluate_plan_context(config: &Config) -> Result<PlanContext> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let plan = tiny_live_activation_plan::evaluate_tiny_live_activation_plan(
        &tiny_live_plan_config(config),
        &loaded_config,
    )
    .await?;
    let source_fingerprint =
        activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)?;
    Ok(PlanContext {
        loaded_config,
        plan,
        source_fingerprint,
    })
}

fn tiny_live_plan_config(config: &Config) -> tiny_live_activation_plan::Config {
    tiny_live_activation_plan::Config {
        config_path: config.config_path.clone(),
        json: false,
        output_path: None,
        now: config.now,
        stage3_limit: config.stage3_limit,
        stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
        rehearsal_limit: config.rehearsal_limit,
        rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
        min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
    }
}

fn plan_mode_verdict_and_reason(
    plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> (TinyLiveActivationExecuteVerdict, String) {
    if plan.pre_activation_gate.verdict == "blocked_by_stage3" {
        return (
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByStage3,
            format!(
                "Stage 3 remains the hard gate, so activation execution stays blocked: {}",
                plan.pre_activation_gate.reason
            ),
        );
    }
    if !plan.pre_activation_gate.planning_green {
        return (
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByPreActivationGate,
            format!(
                "pre-activation gate remains non-green, so activation execution stays blocked: {}",
                plan.pre_activation_gate.reason
            ),
        );
    }
    if plan.verdict
        != tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
    {
        return (
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByPlanContract,
            format!(
                "activation plan is still missing an explicit bounded execution contract: {}",
                plan.reason
            ),
        );
    }
    (
        TinyLiveActivationExecuteVerdict::TinyLiveActivationPlanReady,
        "bounded activation overlay and rollback overlay are explicit, but this command still does not authorize production activation by itself".to_string(),
    )
}

fn refusal_report(
    config: &Config,
    render_kind: RenderKind,
    context: &PlanContext,
    verdict: TinyLiveActivationExecuteVerdict,
    reason: String,
) -> ExecuteReport {
    ExecuteReport {
        generated_at: config.now,
        mode: match render_kind {
            RenderKind::Activation => "render_activation_config".to_string(),
            RenderKind::Rollback => "render_rollback_config".to_string(),
        },
        verdict,
        reason,
        input_config_path: config.config_path.display().to_string(),
        output_config_path: config
            .output_path
            .as_ref()
            .map(|path| path.display().to_string()),
        metadata_path: config
            .output_path
            .as_ref()
            .map(|path| metadata_path_for_config(path).display().to_string()),
        render_kind: Some(render_kind),
        pre_activation_gate_verdict: Some(context.plan.pre_activation_gate.verdict.clone()),
        pre_activation_gate_reason: Some(context.plan.pre_activation_gate.reason.clone()),
        activation_plan_verdict: Some(serialize_enum(&context.plan.verdict)),
        activation_plan_reason: Some(context.plan.reason.clone()),
        source_config_fingerprint_scope: Some(context.source_fingerprint.scope.clone()),
        source_config_fingerprint_sha256: Some(context.source_fingerprint.sha256.clone()),
        expected_source_fingerprint_sha256: config.expected_source_fingerprint_sha256.clone(),
        overlay_field_count: context.plan.activation_overlay_changes.len(),
        applied_change_count: 0,
        changed_fields: build_target_field_expectations(
            &context.loaded_config,
            &context.plan,
            render_kind,
        )
        .unwrap_or_default(),
        service_restart_contract_complete: Some(context.plan.service_restart_contract_complete),
        rollback_summary: Some(rollback_summary(&context.plan)),
        verification: None,
        activation_authorized: false,
        explicit_statement:
            "this batch builds a bounded executor path only; it does not authorize production activation or mutate the live server config by itself"
                .to_string(),
    }
}

fn refusal_report_for_config_drift(
    config: &Config,
    render_kind: RenderKind,
    context: &PlanContext,
    expected_source_fingerprint_sha256: String,
) -> ExecuteReport {
    let verdict = match render_kind {
        RenderKind::Activation => {
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByConfigDrift
        }
        RenderKind::Rollback => {
            TinyLiveActivationExecuteVerdict::TinyLiveRollbackRefusedByConfigDrift
        }
    };
    refusal_report(
        config,
        render_kind,
        context,
        verdict,
        format!(
            "source config fingerprint drift detected before render: expected {} but loaded {}",
            expected_source_fingerprint_sha256, context.source_fingerprint.sha256
        ),
    )
}

fn build_field_expectations(
    source_config: &AppConfig,
    rendered_config: &AppConfig,
    plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
    render_kind: RenderKind,
) -> Result<Vec<FieldExpectation>> {
    let mut expectations = plan
        .activation_overlay_changes
        .iter()
        .map(|change| {
            let source_value =
                tiny_live_activation_plan::overlay_field_value(source_config, &change.field)?;
            let target_value =
                tiny_live_activation_plan::overlay_field_value(rendered_config, &change.field)?;
            Ok(FieldExpectation {
                field: change.field.clone(),
                source_value,
                target_value,
                reason: change.reason.clone(),
                source: change.source.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if render_kind == RenderKind::Rollback {
        inject_safe_rollback_expectation(source_config, rendered_config, &mut expectations)?;
    }
    Ok(expectations)
}

fn build_target_field_expectations(
    source_config: &AppConfig,
    plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
    render_kind: RenderKind,
) -> Result<Vec<FieldExpectation>> {
    plan.activation_overlay_changes
        .iter()
        .map(|change| {
            let source_value =
                tiny_live_activation_plan::overlay_field_value(source_config, &change.field)?;
            let target_value = match render_kind {
                RenderKind::Activation => change.activation_value.clone(),
                RenderKind::Rollback => change.rollback_value.clone(),
            };
            Ok(FieldExpectation {
                field: change.field.clone(),
                source_value,
                target_value,
                reason: change.reason.clone(),
                source: change.source.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()
        .and_then(|mut expectations| {
            if render_kind == RenderKind::Rollback {
                let mut rendered_config = source_config.clone();
                rendered_config.execution.enabled = false;
                inject_safe_rollback_expectation(
                    source_config,
                    &rendered_config,
                    &mut expectations,
                )?;
            }
            Ok(expectations)
        })
}

fn verify_field_expectations(
    config: &AppConfig,
    field_expectations: &[FieldExpectation],
) -> Result<()> {
    let mismatches = collect_field_expectation_mismatches(config, field_expectations)?;
    if mismatches.is_empty() {
        Ok(())
    } else {
        bail!("{}", mismatches.join("; "))
    }
}

fn collect_field_expectation_mismatches(
    config: &AppConfig,
    field_expectations: &[FieldExpectation],
) -> Result<Vec<String>> {
    let mut mismatches = Vec::new();
    for field in field_expectations {
        let actual = tiny_live_activation_plan::overlay_field_value(config, &field.field)?;
        if actual != field.target_value {
            mismatches.push(format!(
                "field {} mismatch: expected {} but rendered {}",
                field.field, field.target_value, actual
            ));
        }
    }
    Ok(mismatches)
}

fn inject_safe_rollback_expectation(
    source_config: &AppConfig,
    rendered_config: &AppConfig,
    field_expectations: &mut Vec<FieldExpectation>,
) -> Result<()> {
    let source_value =
        tiny_live_activation_plan::overlay_field_value(source_config, "execution.enabled")?;
    let target_value =
        tiny_live_activation_plan::overlay_field_value(rendered_config, "execution.enabled")?;
    if let Some(existing) = field_expectations
        .iter_mut()
        .find(|field| field.field == "execution.enabled")
    {
        existing.source_value = source_value;
        existing.target_value = target_value;
        existing.reason =
            "rollback must always restore execution.enabled=false before any later prod-facing decision"
                .to_string();
        existing.source = "forced_safe_disable".to_string();
    } else {
        field_expectations.push(FieldExpectation {
            field: "execution.enabled".to_string(),
            source_value,
            target_value,
            reason:
                "rollback must always restore execution.enabled=false before any later prod-facing decision"
                    .to_string(),
            source: "forced_safe_disable".to_string(),
        });
    }
    Ok(())
}

fn render_config_toml(
    source_toml: &str,
    field_expectations: &[FieldExpectation],
) -> Result<String> {
    let mut root: TomlValue = source_toml
        .parse::<TomlValue>()
        .context("failed parsing source config TOML")?;
    for field in field_expectations {
        set_toml_path(&mut root, &field.field, &field.target_value)?;
    }
    toml::to_string_pretty(&root).context("failed serializing rendered TOML")
}

fn set_toml_path(root: &mut TomlValue, dotted_path: &str, value: &Value) -> Result<()> {
    let segments = dotted_path.split('.').collect::<Vec<_>>();
    if segments.is_empty() {
        bail!("empty TOML path");
    }
    let mut cursor = root
        .as_table_mut()
        .ok_or_else(|| anyhow!("source TOML root must be a table"))?;
    for segment in &segments[..segments.len() - 1] {
        let entry = cursor
            .entry(segment.to_string())
            .or_insert_with(|| TomlValue::Table(TomlMap::new()));
        cursor = entry
            .as_table_mut()
            .ok_or_else(|| anyhow!("TOML path segment {segment} is not a table"))?;
    }
    cursor.insert(
        segments
            .last()
            .ok_or_else(|| anyhow!("missing TOML leaf segment"))?
            .to_string(),
        json_to_toml_value(value)?,
    );
    Ok(())
}

fn json_to_toml_value(value: &Value) -> Result<TomlValue> {
    Ok(match value {
        Value::Null => bail!("null values are not supported in rendered TOML"),
        Value::Bool(flag) => TomlValue::Boolean(*flag),
        Value::Number(number) => {
            if let Some(raw) = number.as_i64() {
                TomlValue::Integer(raw)
            } else if let Some(raw) = number.as_u64() {
                let converted = i64::try_from(raw)
                    .with_context(|| format!("u64 value {raw} exceeds TOML integer range"))?;
                TomlValue::Integer(converted)
            } else if let Some(raw) = number.as_f64() {
                TomlValue::Float(raw)
            } else {
                bail!("unsupported numeric value in rendered TOML")
            }
        }
        Value::String(raw) => TomlValue::String(raw.clone()),
        Value::Array(items) => TomlValue::Array(
            items
                .iter()
                .map(json_to_toml_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(map) => {
            let mut table = TomlMap::new();
            for (key, item) in map {
                table.insert(key.clone(), json_to_toml_value(item)?);
            }
            TomlValue::Table(table)
        }
    })
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating parent dir for {}", path.display()))?;
        }
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!(
            "refusing to overwrite existing rendered artifact {}",
            path.display()
        );
    }
    Ok(())
}

fn metadata_path_for_config(path: &Path) -> PathBuf {
    let suffix = format!(
        "{}{}",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("rendered_config"),
        METADATA_SUFFIX
    );
    path.with_file_name(suffix)
}

fn rollback_summary(plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport) -> String {
    format!(
        "rollback remains deterministic and bounded; service restart contract complete={}, rollback overlay complete={}",
        plan.service_restart_contract_complete, plan.rollback_plan_complete
    )
}

fn render_with_context(
    config: &Config,
    render_kind: RenderKind,
    context: PlanContext,
) -> Result<ExecuteReport> {
    let target_verdict = match render_kind {
        RenderKind::Activation => TinyLiveActivationExecuteVerdict::TinyLiveActivationRendered,
        RenderKind::Rollback => TinyLiveActivationExecuteVerdict::TinyLiveRollbackRendered,
    };
    if let Some(expected) = config.expected_source_fingerprint_sha256.as_deref() {
        if expected != context.source_fingerprint.sha256 {
            return Ok(refusal_report_for_config_drift(
                config,
                render_kind,
                &context,
                expected.to_string(),
            ));
        }
    }

    match render_kind {
        RenderKind::Activation => {
            if context.plan.pre_activation_gate.verdict == "blocked_by_stage3" {
                return Ok(refusal_report(
                    config,
                    render_kind,
                    &context,
                    TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByStage3,
                    format!(
                        "activation render refused because Stage 3 remains non-green: {}",
                        context.plan.pre_activation_gate.reason
                    ),
                ));
            }
            if !context.plan.pre_activation_gate.planning_green {
                return Ok(refusal_report(
                    config,
                    render_kind,
                    &context,
                    TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByPreActivationGate,
                    format!(
                        "activation render refused because pre-activation gate is non-green: {}",
                        context.plan.pre_activation_gate.reason
                    ),
                ));
            }
            if context.plan.verdict
                != tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
            {
                return Ok(refusal_report(
                    config,
                    render_kind,
                    &context,
                    TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByPlanContract,
                    format!(
                        "activation render refused because activation plan is not executable yet: {}",
                        context.plan.reason
                    ),
                ));
            }
        }
        RenderKind::Rollback => {
            if !context.plan.rollback_plan_complete
                || !context.plan.service_restart_contract_complete
            {
                return Ok(refusal_report(
                    config,
                    render_kind,
                    &context,
                    TinyLiveActivationExecuteVerdict::TinyLiveRollbackRefusedByPlanContract,
                    format!(
                        "rollback render refused because rollback contract is incomplete: {}",
                        context.plan.reason
                    ),
                ));
            }
        }
    }

    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("render modes require --output"))?;
    let metadata_path = metadata_path_for_config(output_path);
    ensure_new_output_path(output_path)?;
    ensure_new_output_path(&metadata_path)?;

    let mut rendered_config = tiny_live_activation_plan::apply_overlay_from_plan(
        context.loaded_config.clone(),
        &context.plan.activation_overlay_changes,
        match render_kind {
            RenderKind::Activation => tiny_live_activation_plan::OverlayTarget::Activation,
            RenderKind::Rollback => tiny_live_activation_plan::OverlayTarget::Rollback,
        },
    )?;
    if render_kind == RenderKind::Rollback {
        // Rollback must remain able to disable execution deterministically even if the current source
        // config already drifted into an activated posture and no longer carries an execution.enabled delta.
        rendered_config.execution.enabled = false;
    }
    let field_expectations = build_field_expectations(
        &context.loaded_config,
        &rendered_config,
        &context.plan,
        render_kind,
    )?;
    let applied_change_count = field_expectations
        .iter()
        .filter(|field| field.source_value != field.target_value)
        .count();
    let source_toml = fs::read_to_string(&config.config_path).with_context(|| {
        format!(
            "failed reading source config TOML {}",
            config.config_path.display()
        )
    })?;
    let rendered_toml = render_config_toml(&source_toml, &field_expectations)?;
    write_text_file(output_path, &rendered_toml)?;
    let rendered_bytes = rendered_toml.as_bytes().to_vec();
    let rendered_config_sha256 = sha256_hex(&rendered_bytes);

    let written_rendered_config = load_from_path(output_path).with_context(|| {
        format!(
            "failed reloading rendered config {} for verification",
            output_path.display()
        )
    })?;
    verify_field_expectations(&written_rendered_config, &field_expectations)
        .context("rendered config does not satisfy planned overlay fields")?;

    let metadata = RenderedConfigMetadata {
        metadata_version: METADATA_VERSION.to_string(),
        render_kind,
        generated_at: config.now,
        input_config_path: config.config_path.display().to_string(),
        output_config_path: output_path.display().to_string(),
        source_config_fingerprint_scope: context.source_fingerprint.scope.clone(),
        source_config_fingerprint_sha256: context.source_fingerprint.sha256.clone(),
        expected_source_fingerprint_sha256: config.expected_source_fingerprint_sha256.clone(),
        rendered_config_sha256,
        pre_activation_gate_verdict: context.plan.pre_activation_gate.verdict.clone(),
        pre_activation_gate_reason: context.plan.pre_activation_gate.reason.clone(),
        activation_plan_verdict: serialize_enum(&context.plan.verdict),
        activation_plan_reason: context.plan.reason.clone(),
        activation_overlay_complete: context.plan.activation_overlay_complete,
        rollback_plan_complete: context.plan.rollback_plan_complete,
        service_restart_contract_complete: context.plan.service_restart_contract_complete,
        field_expectations: field_expectations.clone(),
        execution_untouched_by_batch: true,
        activation_authorized: false,
        not_authorized_summary:
            "rendering a bounded tiny-live config does not authorize production activation; Stage 3 and the consolidated pre-activation gate still remain the hard gate at decision time"
                .to_string(),
    };
    write_text_file(&metadata_path, &serde_json::to_string_pretty(&metadata)?)?;

    Ok(ExecuteReport {
        generated_at: config.now,
        mode: match render_kind {
            RenderKind::Activation => "render_activation_config".to_string(),
            RenderKind::Rollback => "render_rollback_config".to_string(),
        },
        verdict: target_verdict,
        reason: match render_kind {
            RenderKind::Activation => {
                "bounded tiny-live activation config rendered from accepted activation-plan truth"
                    .to_string()
            }
            RenderKind::Rollback => {
                "bounded tiny-live rollback config rendered from accepted activation-plan truth"
                    .to_string()
            }
        },
        input_config_path: config.config_path.display().to_string(),
        output_config_path: Some(output_path.display().to_string()),
        metadata_path: Some(metadata_path.display().to_string()),
        render_kind: Some(render_kind),
        pre_activation_gate_verdict: Some(context.plan.pre_activation_gate.verdict.clone()),
        pre_activation_gate_reason: Some(context.plan.pre_activation_gate.reason.clone()),
        activation_plan_verdict: Some(serialize_enum(&context.plan.verdict)),
        activation_plan_reason: Some(context.plan.reason.clone()),
        source_config_fingerprint_scope: Some(context.source_fingerprint.scope),
        source_config_fingerprint_sha256: Some(context.source_fingerprint.sha256),
        expected_source_fingerprint_sha256: config.expected_source_fingerprint_sha256.clone(),
        overlay_field_count: field_expectations.len(),
        applied_change_count,
        changed_fields: field_expectations,
        service_restart_contract_complete: Some(context.plan.service_restart_contract_complete),
        rollback_summary: Some(rollback_summary(&context.plan)),
        verification: None,
        activation_authorized: false,
        explicit_statement:
            "this batch renders bounded activation or rollback configs only; it does not enable production execution or authorize a production service restart"
                .to_string(),
    })
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn render_human(report: &ExecuteReport) -> String {
    let mut lines = vec![
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("mode={}", report.mode),
        format!("reason={}", report.reason),
        format!("input_config_path={}", report.input_config_path),
    ];
    if let Some(path) = &report.output_config_path {
        lines.push(format!("output_config_path={path}"));
    }
    if let Some(path) = &report.metadata_path {
        lines.push(format!("metadata_path={path}"));
    }
    if let Some(kind) = report.render_kind {
        lines.push(format!("render_kind={}", serialize_enum(&kind)));
    }
    if let Some(verdict) = &report.pre_activation_gate_verdict {
        lines.push(format!("pre_activation_gate_verdict={verdict}"));
    }
    if let Some(reason) = &report.pre_activation_gate_reason {
        lines.push(format!("pre_activation_gate_reason={reason}"));
    }
    if let Some(verdict) = &report.activation_plan_verdict {
        lines.push(format!("activation_plan_verdict={verdict}"));
    }
    if let Some(reason) = &report.activation_plan_reason {
        lines.push(format!("activation_plan_reason={reason}"));
    }
    if let Some(scope) = &report.source_config_fingerprint_scope {
        lines.push(format!("source_config_fingerprint_scope={scope}"));
    }
    if let Some(sha) = &report.source_config_fingerprint_sha256 {
        lines.push(format!("source_config_fingerprint_sha256={sha}"));
    }
    if let Some(expected) = &report.expected_source_fingerprint_sha256 {
        lines.push(format!("expected_source_fingerprint_sha256={expected}"));
    }
    lines.push(format!(
        "overlay_field_count={}",
        report.overlay_field_count
    ));
    lines.push(format!(
        "applied_change_count={}",
        report.applied_change_count
    ));
    if !report.changed_fields.is_empty() {
        lines.push("changed_fields:".to_string());
        lines.extend(report.changed_fields.iter().map(|field| {
            format!(
                "  {}: {} -> {} ({})",
                field.field, field.source_value, field.target_value, field.reason
            )
        }));
    }
    if let Some(summary) = &report.rollback_summary {
        lines.push(format!("rollback_summary={summary}"));
    }
    if let Some(verification) = &report.verification {
        lines.push(format!(
            "verification_file_hash_matches_metadata={}",
            verification.file_hash_matches_metadata
        ));
        lines.push(format!(
            "verification_source_config_path_present={}",
            verification.source_config_path_present
        ));
        if let Some(matches) = verification.source_config_fingerprint_matches_current {
            lines.push(format!(
                "verification_source_config_fingerprint_matches_current={matches}"
            ));
        }
        if !verification.metadata_mismatches.is_empty() {
            lines.push("verification_metadata_mismatches:".to_string());
            lines.extend(
                verification
                    .metadata_mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
        if !verification.field_mismatches.is_empty() {
            lines.push("verification_field_mismatches:".to_string());
            lines.extend(
                verification
                    .field_mismatches
                    .iter()
                    .map(|entry| format!("  {entry}")),
            );
        }
    }
    lines.push("activation_authorized=false".to_string());
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
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

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("invalid RFC3339 timestamp for {flag}: {raw}"))?
        .with_timezone(&Utc))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid usize for {flag}: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid u64 for {flag}: {raw}"))
}

fn normalize_sha256_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = parse_string_arg(flag, value)?.to_ascii_lowercase();
    if raw.len() != 64 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{flag} must be a 64-character hexadecimal sha256");
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn green_planning_truth_renders_bounded_activation_config_successfully() {
        let temp_dir = temp_dir("tiny_live_activation_execute_render_activation");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("rendered.activation.toml");
        fs::write(&config_path, sample_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = ready_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let config = sample_render_config(
            Mode::RenderActivationConfig,
            config_path.clone(),
            Some(output_path.clone()),
            Some(source_fingerprint.sha256.clone()),
        );

        let report = build_render_report_from_context(
            &config,
            RenderKind::Activation,
            PlanContext {
                loaded_config: loaded_config.clone(),
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        assert_eq!(
            report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRendered
        );
        assert_eq!(report.render_kind, Some(RenderKind::Activation));
        let rendered = load_from_path(&output_path).unwrap();
        assert!(rendered.execution.enabled);
        assert_eq!(rendered.execution.default_route, "jito");
        assert_eq!(
            rendered.execution.submit_allowed_routes,
            vec!["jito".to_string()]
        );
        assert_eq!(rendered.execution.batch_size, 1);
        assert_eq!(rendered.risk.max_concurrent_positions, 1);
        assert_eq!(rendered.risk.daily_loss_limit_pct, 0.75);
        assert_eq!(
            load_from_path(&config_path).unwrap().execution.enabled,
            loaded_config.execution.enabled
        );
        assert!(metadata_path_for_config(&output_path).exists());
    }

    #[test]
    fn green_planning_truth_renders_bounded_rollback_config_successfully() {
        let temp_dir = temp_dir("tiny_live_activation_execute_render_rollback");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("rendered.rollback.toml");
        fs::write(&config_path, sample_activated_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = ready_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let config = sample_render_config(
            Mode::RenderRollbackConfig,
            config_path.clone(),
            Some(output_path.clone()),
            Some(source_fingerprint.sha256.clone()),
        );

        let report = build_render_report_from_context(
            &config,
            RenderKind::Rollback,
            PlanContext {
                loaded_config,
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        assert_eq!(
            report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveRollbackRendered
        );
        let rendered = load_from_path(&output_path).unwrap();
        assert!(!rendered.execution.enabled);
        assert_eq!(rendered.execution.default_route, "jito");
        assert_eq!(rendered.execution.batch_size, 1);
        assert_eq!(rendered.risk.max_concurrent_positions, 1);
        assert_eq!(rendered.risk.daily_loss_limit_pct, 0.75);
    }

    #[test]
    fn stage3_non_green_blocks_activation_render_for_prod_facing_execution() {
        let temp_dir = temp_dir("tiny_live_activation_execute_stage3_block");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("blocked.activation.toml");
        fs::write(&config_path, sample_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = blocked_stage3_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let config = sample_render_config(
            Mode::RenderActivationConfig,
            config_path.clone(),
            Some(output_path),
            Some(source_fingerprint.sha256.clone()),
        );

        let report = build_render_report_from_context(
            &config,
            RenderKind::Activation,
            PlanContext {
                loaded_config,
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        assert_eq!(
            report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByStage3
        );
    }

    #[test]
    fn config_drift_fingerprint_mismatch_is_refused_explicitly() {
        let temp_dir = temp_dir("tiny_live_activation_execute_drift");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("drift.activation.toml");
        fs::write(&config_path, sample_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = ready_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let config = sample_render_config(
            Mode::RenderActivationConfig,
            config_path.clone(),
            Some(output_path),
            Some(format!("{}0", &source_fingerprint.sha256[..63])),
        );

        let report = build_render_report_from_context(
            &config,
            RenderKind::Activation,
            PlanContext {
                loaded_config,
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        assert_eq!(
            report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveActivationRefusedByConfigDrift
        );
    }

    #[test]
    fn verify_mode_catches_malformed_or_manually_drifted_rendered_config() {
        let temp_dir = temp_dir("tiny_live_activation_execute_verify_drift");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("rendered.activation.toml");
        fs::write(&config_path, sample_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = ready_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let render_config = sample_render_config(
            Mode::RenderActivationConfig,
            config_path.clone(),
            Some(output_path.clone()),
            Some(source_fingerprint.sha256.clone()),
        );
        build_render_report_from_context(
            &render_config,
            RenderKind::Activation,
            PlanContext {
                loaded_config,
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        let tampered = sample_config_toml().replace("enabled = false", "enabled = true");
        fs::write(&output_path, tampered).unwrap();

        let verify_report = build_verify_report(&Config {
            mode: Mode::VerifyRenderedConfig,
            config_path: output_path.clone(),
            output_path: None,
            expected_source_fingerprint_sha256: None,
            json: false,
            now: ts("2026-03-27T12:00:00Z"),
            stage3_limit: 3,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: 3,
            rehearsal_recent_horizon_seconds: 3600,
            min_recent_acceptable_rehearsals: 1,
        })
        .unwrap();

        assert_eq!(
            verify_report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveActivationVerifyInvalid
        );
        assert!(
            !verify_report
                .verification
                .as_ref()
                .unwrap()
                .metadata_mismatches
                .is_empty()
                || !verify_report
                    .verification
                    .as_ref()
                    .unwrap()
                    .field_mismatches
                    .is_empty()
        );
    }

    #[test]
    fn verify_mode_accepts_unchanged_rendered_activation_config() {
        let temp_dir = temp_dir("tiny_live_activation_execute_verify_ok");
        let config_path = temp_dir.join("live.server.toml");
        let output_path = temp_dir.join("rendered.activation.toml");
        fs::write(&config_path, sample_config_toml()).unwrap();
        let loaded_config = load_from_path(&config_path).unwrap();
        let plan = ready_plan_report(&config_path, &loaded_config);
        let source_fingerprint =
            activation_decision_packet::build_config_fingerprint(FINGERPRINT_SCOPE, &loaded_config)
                .unwrap();
        let render_config = sample_render_config(
            Mode::RenderActivationConfig,
            config_path.clone(),
            Some(output_path.clone()),
            Some(source_fingerprint.sha256.clone()),
        );
        build_render_report_from_context(
            &render_config,
            RenderKind::Activation,
            PlanContext {
                loaded_config,
                plan,
                source_fingerprint,
            },
        )
        .unwrap();

        let verify_report = build_verify_report(&Config {
            mode: Mode::VerifyRenderedConfig,
            config_path: output_path,
            output_path: None,
            expected_source_fingerprint_sha256: None,
            json: false,
            now: ts("2026-03-27T12:00:00Z"),
            stage3_limit: 3,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: 3,
            rehearsal_recent_horizon_seconds: 3600,
            min_recent_acceptable_rehearsals: 1,
        })
        .unwrap();

        assert_eq!(
            verify_report.verdict,
            TinyLiveActivationExecuteVerdict::TinyLiveActivationVerifyOk
        );
    }

    #[test]
    fn parse_args_rejects_expected_fingerprint_outside_render_modes() {
        let error = parse_args_from([
            "--config".to_string(),
            "/tmp/live.server.toml".to_string(),
            "--verify-rendered-config".to_string(),
            "--expected-source-fingerprint".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        ])
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("--expected-source-fingerprint is only valid with render modes"));
    }

    fn build_render_report_from_context(
        config: &Config,
        render_kind: RenderKind,
        context: PlanContext,
    ) -> Result<ExecuteReport> {
        render_with_context(config, render_kind, context)
    }

    fn sample_render_config(
        mode: Mode,
        config_path: PathBuf,
        output_path: Option<PathBuf>,
        expected_source_fingerprint_sha256: Option<String>,
    ) -> Config {
        Config {
            mode,
            config_path,
            output_path,
            expected_source_fingerprint_sha256,
            json: false,
            now: ts("2026-03-27T12:00:00Z"),
            stage3_limit: 3,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: 3,
            rehearsal_recent_horizon_seconds: 3600,
            min_recent_acceptable_rehearsals: 1,
        }
    }

    fn ready_plan_report(
        config_path: &Path,
        loaded_config: &AppConfig,
    ) -> tiny_live_activation_plan::TinyLiveActivationPlanReport {
        tiny_live_activation_plan::build_activation_plan_report(
            &tiny_live_activation_plan::Config {
                config_path: config_path.to_path_buf(),
                json: false,
                output_path: None,
                now: ts("2026-03-27T12:00:00Z"),
                stage3_limit: 3,
                stage3_recent_horizon_seconds: None,
                rehearsal_limit: 3,
                rehearsal_recent_horizon_seconds: 3600,
                min_recent_acceptable_rehearsals: 1,
            },
            loaded_config,
            pre_activation_report(
                tiny_live_activation_plan::pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        )
    }

    fn blocked_stage3_plan_report(
        config_path: &Path,
        loaded_config: &AppConfig,
    ) -> tiny_live_activation_plan::TinyLiveActivationPlanReport {
        tiny_live_activation_plan::build_activation_plan_report(
            &tiny_live_activation_plan::Config {
                config_path: config_path.to_path_buf(),
                json: false,
                output_path: None,
                now: ts("2026-03-27T12:00:00Z"),
                stage3_limit: 3,
                stage3_recent_horizon_seconds: None,
                rehearsal_limit: 3,
                rehearsal_recent_horizon_seconds: 3600,
                min_recent_acceptable_rehearsals: 1,
            },
            loaded_config,
            pre_activation_report(
                tiny_live_activation_plan::pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage3,
                "stage3 still accumulating",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        )
    }

    fn pre_activation_report(
        verdict: tiny_live_activation_plan::pre_activation_gate_report::PreActivationGateVerdict,
        reason: &str,
    ) -> tiny_live_activation_plan::pre_activation_gate_report::PreActivationGateReport {
        tiny_live_activation_plan::pre_activation_gate_report::PreActivationGateReport {
            generated_at: ts("2026-03-27T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            db_path: "/tmp/live_runtime.db".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            activation_permission_granted: false,
            stage3_is_primary_gate: true,
            verdict,
            reason: reason.to_string(),
            blockers: Vec::new(),
            stage3: tiny_live_activation_plan::pre_activation_gate_report::Stage3GateSummary {
                verdict: "validated_current".to_string(),
                reason: "validated".to_string(),
                stage3_green: true,
                captures_loaded: 3,
                captures_within_recent_horizon: 3,
                recent_horizon_seconds: 3600,
                latest_capture_age_seconds: Some(60),
                stale_captures_excluded_from_verdict: false,
                exact_published_current_match_count: 3,
                exact_active_current_match_count: 3,
                rotation_evidence_capture_count: 3,
                shadow_signal_present_capture_count: 3,
            },
            stage4_readiness: tiny_live_activation_plan::pre_activation_gate_report::Stage4ReadinessSummary {
                verdict: "ready_for_execution_dry_run".to_string(),
                reason: "ready".to_string(),
                config_valid: true,
                connectivity_valid: true,
                adapter_contract_valid: true,
                signer_contract_valid: true,
                policy_contract_valid: true,
                route_contract_valid: true,
                ready_for_dry_run: true,
                blocked_for_activation: true,
            },
            stage4_dry_run_history: tiny_live_activation_plan::pre_activation_gate_report::DryRunHistorySummary {
                verdict:
                    tiny_live_activation_plan::pre_activation_gate_report::DryRunHistoryVerdict::SufficientRecentRehearsalEvidence,
                reason: "sufficient".to_string(),
                records_loaded: 3,
                recent_rehearsals_within_horizon: 3,
                recent_horizon_seconds: 3600,
                latest_rehearsal_age_seconds: Some(60),
                stale_rehearsals_excluded_from_verdict: false,
                stale_rehearsals_excluded_count: 0,
                acceptable_recent_rehearsal_count: 3,
                recent_hard_blocker_count: 0,
                latest_recent_verdict: Some("rehearsal_green".to_string()),
                verdict_counts: [("rehearsal_green".to_string(), 3)]
                    .into_iter()
                    .collect(),
            },
            tiny_live_policy: tiny_live_activation_plan::pre_activation_gate_report::TinyLivePolicySummary {
                verdict: "tiny_live_policy_bounded".to_string(),
                reason: "bounded".to_string(),
                tiny_live_policy_bounded: true,
                tiny_live_policy_enabled: true,
                blocker_count: 0,
                first_blocker: None,
                warnings_count: 0,
                mode_compatible: true,
                execution_policy_contract_valid: true,
                execution_route_contract_valid: true,
            },
        }
    }

    fn bounded_policy_report(
    ) -> tiny_live_activation_plan::tiny_live_policy_audit::TinyLivePolicyAuditReport {
        tiny_live_activation_plan::tiny_live_policy_audit::TinyLivePolicyAuditReport {
            generated_at: ts("2026-03-27T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            stage3_gate_not_evaluated: true,
            mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            execution_policy_contract_valid: true,
            execution_route_contract_valid: true,
            current_config_bounded_for_later_tiny_live_discussion: true,
            suitable_only_for_paper_or_dry_run: false,
            verdict: tiny_live_activation_plan::tiny_live_policy_audit::TinyLivePolicyVerdict::TinyLivePolicyBounded,
            reason: "bounded".to_string(),
            blockers: Vec::new(),
            warnings: vec!["execution.enabled=false".to_string()],
            tiny_live_policy_enabled: true,
            current_default_route: "jito".to_string(),
            current_allowed_routes: vec!["jito".to_string()],
            current_route_order: vec!["jito".to_string()],
            policy_allowed_routes: vec!["jito".to_string()],
            current_shadow_copy_notional_sol: 0.05,
            current_risk_max_position_sol: 0.05,
            policy_max_trade_notional_sol: 0.05,
            current_execution_batch_size: 1,
            policy_max_batch_size: 1,
            current_risk_max_concurrent_positions: 1,
            policy_max_concurrent_positions: 1,
            current_risk_daily_loss_limit_pct: 0.75,
            policy_max_daily_loss_limit_pct: 1.0,
            current_pretrade_max_fee_overhead_bps: 800,
            policy_max_pretrade_fee_overhead_bps: 1000,
            current_pretrade_max_priority_fee_lamports: 1500,
            policy_max_pretrade_priority_fee_lamports: 2000,
            current_policy_echo_required: true,
            policy_echo_required_for_tiny_live: true,
            route_policy_rows: vec![
                tiny_live_activation_plan::tiny_live_policy_audit::RoutePolicyAuditRow {
                    route: "jito".to_string(),
                    current_allowed: true,
                    policy_allowed: true,
                    current_route_ordered: true,
                    current_slippage_bps: Some(40.0),
                    policy_max_slippage_bps: Some(50.0),
                    slippage_within_bound: true,
                    current_tip_lamports: Some(10_000),
                    policy_max_tip_lamports: Some(10_000),
                    tip_within_bound: true,
                    current_compute_unit_limit: Some(300_000),
                    current_compute_unit_price_micro_lamports: Some(1_500),
                    policy_max_compute_unit_price_micro_lamports: Some(1_500),
                    compute_unit_price_within_bound: true,
                },
            ],
        }
    }

    fn bounded_guardrail_report(
    ) -> tiny_live_activation_plan::tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport {
        tiny_live_activation_plan::tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport {
            generated_at: ts("2026-03-27T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            activation_permission_granted: false,
            stage3_gate_not_evaluated: true,
            current_execution_mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
            tiny_live_policy_reason: "bounded".to_string(),
            tiny_live_policy_bounded: true,
            tiny_live_guardrails_enabled: true,
            monitoring_contract_complete: true,
            rollback_contract_complete: true,
            current_config_bounded_for_later_tiny_live_monitoring: true,
            verdict: tiny_live_activation_plan::tiny_live_guardrail_audit::TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded,
            reason: "guardrails bounded".to_string(),
            blockers: Vec::new(),
            warnings: vec!["execution.enabled=false".to_string()],
            evaluation_window_seconds: 900,
            max_execution_error_rate_pct: 5.0,
            max_adapter_contract_failure_rate_pct: 1.0,
            max_policy_echo_mismatch_rate_pct: 1.0,
            max_fee_or_slippage_breach_rate_pct: 5.0,
            max_connectivity_degraded_window_seconds: 120,
            max_daily_realized_loss_sol: 0.05,
            max_consecutive_hard_failures: 3,
            reference_envelope: tiny_live_activation_plan::tiny_live_guardrail_audit::GuardrailReferenceEnvelope {
                max_evaluation_window_seconds: 900,
                max_execution_error_rate_pct: 5.0,
                max_adapter_contract_failure_rate_pct: 1.0,
                max_policy_echo_mismatch_rate_pct: 1.0,
                max_fee_or_slippage_breach_rate_pct: 5.0,
                max_connectivity_degraded_window_seconds: 180,
                max_daily_realized_loss_sol: 0.05,
                max_consecutive_hard_failures: 3,
            },
            rollback_triggers: vec![
                tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary {
                    trigger: "execution_error_rate".to_string(),
                    threshold_kind: "rate_pct".to_string(),
                    threshold_rate_pct: Some(5.0),
                    threshold_seconds: None,
                    threshold_sol: None,
                    threshold_count: None,
                    evaluation_window_seconds: Some(900),
                    action: "mandatory_rollback".to_string(),
                },
            ],
        }
    }

    fn sample_config_toml() -> String {
        r#"
[system]
env = "prod-live"

[execution]
enabled = false
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

    fn sample_activated_config_toml() -> String {
        sample_config_toml().replace("enabled = false", "enabled = true")
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
