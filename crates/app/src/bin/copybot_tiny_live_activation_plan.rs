use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_pre_activation_gate_report.rs"]
mod pre_activation_gate_report;
#[allow(dead_code)]
#[path = "copybot_tiny_live_guardrail_audit.rs"]
mod tiny_live_guardrail_audit;
#[allow(dead_code)]
#[path = "copybot_tiny_live_policy_audit.rs"]
mod tiny_live_policy_audit;

const USAGE: &str = "usage: copybot_tiny_live_activation_plan --config <path> [--json] [--output <path>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>]";
pub(crate) const DEFAULT_REHEARSAL_HISTORY_LIMIT: usize = 10;
pub(crate) const DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS: u64 = 86_400;
pub(crate) const DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS: usize = 2;
const TARGET_EXECUTION_MODE: &str = "adapter_submit_confirm";
const TARGET_SERVICE_NAME: &str = "solana-copy-bot.service";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for tiny-live activation plan")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) json: bool,
    pub(crate) output_path: Option<PathBuf>,
    pub(crate) now: DateTime<Utc>,
    pub(crate) stage3_limit: usize,
    pub(crate) stage3_recent_horizon_seconds: Option<u64>,
    pub(crate) rehearsal_limit: usize,
    pub(crate) rehearsal_recent_horizon_seconds: u64,
    pub(crate) min_recent_acceptable_rehearsals: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TinyLiveActivationPlanVerdict {
    ActivationPlanReadyWhenStageGateAllows,
    BlockedByPreActivationGate,
    BlockedByPolicyContract,
    BlockedByGuardrailContract,
    ActivationOverlayIncomplete,
    RollbackPlanIncomplete,
    ServiceRestartContractIncomplete,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActivationPlanGateSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) planning_green: bool,
    pub(crate) blockers: Vec<String>,
    pub(crate) execution_enabled: bool,
    pub(crate) stage3_is_primary_gate: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PolicyAuditSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) bounded: bool,
    pub(crate) tiny_live_policy_enabled: bool,
    pub(crate) blocker_count: usize,
    pub(crate) first_blocker: Option<String>,
    pub(crate) warnings_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GuardrailAuditSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) bounded: bool,
    pub(crate) tiny_live_guardrails_enabled: bool,
    pub(crate) first_blocker: Option<String>,
    pub(crate) blocker_count: usize,
    pub(crate) warnings_count: usize,
    pub(crate) rollback_triggers: Vec<tiny_live_guardrail_audit::RollbackTriggerSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActivationPlanFieldDelta {
    pub(crate) field: String,
    pub(crate) current_value: Value,
    pub(crate) activation_value: Value,
    pub(crate) rollback_value: Value,
    pub(crate) reason: String,
    pub(crate) source: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UnchangedFieldSummary {
    pub(crate) field: String,
    pub(crate) value: Value,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RouteEnvelopeRow {
    pub(crate) route: String,
    pub(crate) compute_unit_limit: u32,
    pub(crate) slippage_bps: f64,
    pub(crate) tip_lamports: u64,
    pub(crate) compute_unit_price_micro_lamports: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EffectiveTinyLiveContract {
    pub(crate) execution_mode: String,
    pub(crate) execution_enabled_target: bool,
    pub(crate) default_route: String,
    pub(crate) allowed_routes: Vec<String>,
    pub(crate) route_order: Vec<String>,
    pub(crate) policy_echo_required: bool,
    pub(crate) shadow_copy_notional_sol: f64,
    pub(crate) risk_max_position_sol: f64,
    pub(crate) execution_batch_size: u32,
    pub(crate) risk_max_concurrent_positions: u32,
    pub(crate) risk_daily_loss_limit_pct: f64,
    pub(crate) pretrade_max_fee_overhead_bps: u32,
    pub(crate) pretrade_max_priority_fee_lamports: u64,
    pub(crate) route_envelope: Vec<RouteEnvelopeRow>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ServiceRestartContract {
    pub(crate) complete: bool,
    pub(crate) activation_restart_required: bool,
    pub(crate) rollback_restart_required: bool,
    pub(crate) activation_services: Vec<String>,
    pub(crate) rollback_services: Vec<String>,
    pub(crate) activation_steps: Vec<String>,
    pub(crate) rollback_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TinyLiveActivationPlanReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) output_path: Option<String>,
    pub(crate) planning_safe_only: bool,
    pub(crate) activation_permission_granted: bool,
    pub(crate) execution_enabled_current: bool,
    pub(crate) current_execution_mode: String,
    pub(crate) verdict: TinyLiveActivationPlanVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) drift_findings: Vec<String>,
    pub(crate) pre_activation_gate: ActivationPlanGateSummary,
    pub(crate) tiny_live_policy: PolicyAuditSummary,
    pub(crate) tiny_live_guardrails: GuardrailAuditSummary,
    pub(crate) activation_overlay_complete: bool,
    pub(crate) rollback_plan_complete: bool,
    pub(crate) service_restart_contract_complete: bool,
    pub(crate) activation_overlay_change_count: usize,
    pub(crate) rollback_overlay_change_count: usize,
    pub(crate) activation_overlay_changes: Vec<ActivationPlanFieldDelta>,
    pub(crate) unchanged_fields: Vec<UnchangedFieldSummary>,
    pub(crate) effective_tiny_live_contract: Option<EffectiveTinyLiveContract>,
    pub(crate) service_restart_contract: ServiceRestartContract,
}

#[derive(Debug, Clone)]
struct OverlayBuildResult {
    complete: bool,
    blockers: Vec<String>,
    drift_findings: Vec<String>,
    activation_overlay_changes: Vec<ActivationPlanFieldDelta>,
    unchanged_fields: Vec<UnchangedFieldSummary>,
    effective_contract: Option<EffectiveTinyLiveContract>,
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
    let mut json = false;
    let mut output_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit = DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds = DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals = DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
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
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        json,
        output_path,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let report = evaluate_tiny_live_activation_plan(&config, &loaded_config).await?;
    let json_output = serde_json::to_string_pretty(&report)
        .context("failed serializing tiny-live activation plan json")?;
    if let Some(output_path) = &config.output_path {
        write_output(output_path, &json_output)?;
    }
    if config.json {
        Ok(json_output)
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) async fn evaluate_tiny_live_activation_plan(
    config: &Config,
    loaded_config: &AppConfig,
) -> Result<TinyLiveActivationPlanReport> {
    let pre_activation_gate = pre_activation_gate_report::evaluate_pre_activation_gate_report(
        &config.config_path,
        config.now,
        config.stage3_limit,
        config.stage3_recent_horizon_seconds,
        config.rehearsal_limit,
        config.rehearsal_recent_horizon_seconds,
        config.min_recent_acceptable_rehearsals,
    )
    .await?;
    let tiny_live_policy =
        tiny_live_policy_audit::evaluate_tiny_live_policy(&config.config_path, loaded_config)?;
    let tiny_live_guardrails = tiny_live_guardrail_audit::evaluate_tiny_live_guardrails(
        &config.config_path,
        loaded_config,
    )?;
    Ok(build_activation_plan_report(
        config,
        loaded_config,
        pre_activation_gate,
        tiny_live_policy,
        tiny_live_guardrails,
    ))
}

fn write_output(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed creating parent dir for output {}", path.display())
            })?;
        }
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing tiny-live activation plan {}",
            path.display()
        )
    })
}

fn build_activation_plan_report(
    config: &Config,
    loaded_config: &AppConfig,
    pre_activation_gate: pre_activation_gate_report::PreActivationGateReport,
    tiny_live_policy: tiny_live_policy_audit::TinyLivePolicyAuditReport,
    tiny_live_guardrails: tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
) -> TinyLiveActivationPlanReport {
    let overlay = build_overlay_plan(loaded_config, &tiny_live_policy);
    let service_restart_contract = build_service_restart_contract();
    let rollback_plan_complete = !overlay.activation_overlay_changes.is_empty()
        || !overlay.unchanged_fields.is_empty()
        || loaded_config.execution.enabled == false;
    let (verdict, reason, mut blockers) = derive_plan_verdict(
        &pre_activation_gate,
        &tiny_live_policy,
        &tiny_live_guardrails,
        &overlay,
        rollback_plan_complete,
        service_restart_contract.complete,
    );

    if !loaded_config.execution.enabled {
        blockers.push(
            "execution.enabled=false remains unchanged; this command only generates a future activation candidate"
                .to_string(),
        );
    }
    blockers.sort();
    blockers.dedup();

    TinyLiveActivationPlanReport {
        generated_at: config.now,
        config_path: config.config_path.display().to_string(),
        output_path: config
            .output_path
            .as_ref()
            .map(|path| path.display().to_string()),
        planning_safe_only: true,
        activation_permission_granted: false,
        execution_enabled_current: loaded_config.execution.enabled,
        current_execution_mode: normalize_mode(loaded_config.execution.mode.as_str()),
        verdict,
        reason,
        blockers,
        drift_findings: overlay.drift_findings.clone(),
        pre_activation_gate: ActivationPlanGateSummary {
            verdict: serialize_enum(&pre_activation_gate.verdict),
            reason: pre_activation_gate.reason,
            planning_green: pre_activation_gate.verdict
                == pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
            blockers: pre_activation_gate.blockers,
            execution_enabled: pre_activation_gate.execution_enabled,
            stage3_is_primary_gate: pre_activation_gate.stage3_is_primary_gate,
        },
        tiny_live_policy: PolicyAuditSummary {
            verdict: serialize_enum(&tiny_live_policy.verdict),
            reason: tiny_live_policy.reason,
            bounded: tiny_live_policy.current_config_bounded_for_later_tiny_live_discussion,
            tiny_live_policy_enabled: tiny_live_policy.tiny_live_policy_enabled,
            blocker_count: tiny_live_policy.blockers.len(),
            first_blocker: tiny_live_policy.blockers.first().cloned(),
            warnings_count: tiny_live_policy.warnings.len(),
        },
        tiny_live_guardrails: GuardrailAuditSummary {
            verdict: serialize_enum(&tiny_live_guardrails.verdict),
            reason: tiny_live_guardrails.reason,
            bounded: tiny_live_guardrails.current_config_bounded_for_later_tiny_live_monitoring,
            tiny_live_guardrails_enabled: tiny_live_guardrails.tiny_live_guardrails_enabled,
            first_blocker: tiny_live_guardrails.blockers.first().cloned(),
            blocker_count: tiny_live_guardrails.blockers.len(),
            warnings_count: tiny_live_guardrails.warnings.len(),
            rollback_triggers: tiny_live_guardrails.rollback_triggers,
        },
        activation_overlay_complete: overlay.complete,
        rollback_plan_complete,
        service_restart_contract_complete: service_restart_contract.complete,
        activation_overlay_change_count: overlay.activation_overlay_changes.len(),
        rollback_overlay_change_count: overlay.activation_overlay_changes.len(),
        activation_overlay_changes: overlay.activation_overlay_changes,
        unchanged_fields: overlay.unchanged_fields,
        effective_tiny_live_contract: overlay.effective_contract,
        service_restart_contract,
    }
}

fn derive_plan_verdict(
    pre_activation_gate: &pre_activation_gate_report::PreActivationGateReport,
    tiny_live_policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
    tiny_live_guardrails: &tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
    overlay: &OverlayBuildResult,
    rollback_plan_complete: bool,
    service_restart_contract_complete: bool,
) -> (TinyLiveActivationPlanVerdict, String, Vec<String>) {
    if matches!(
        pre_activation_gate.verdict,
        pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage3
            | pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage4Readiness
            | pre_activation_gate_report::PreActivationGateVerdict::BlockedByDryRunHistory
            | pre_activation_gate_report::PreActivationGateVerdict::InsufficientRecentEvidence
    ) {
        return (
            TinyLiveActivationPlanVerdict::BlockedByPreActivationGate,
            format!(
                "pre-activation gate is not green yet: {}",
                pre_activation_gate.reason
            ),
            vec![format!(
                "pre_activation_gate: {}",
                pre_activation_gate.reason
            )],
        );
    }
    if pre_activation_gate.verdict
        == pre_activation_gate_report::PreActivationGateVerdict::BlockedByTinyLivePolicy
        || !tiny_live_policy.current_config_bounded_for_later_tiny_live_discussion
    {
        return (
            TinyLiveActivationPlanVerdict::BlockedByPolicyContract,
            format!(
                "tiny-live policy envelope is not bounded enough yet: {}",
                tiny_live_policy.reason
            ),
            vec![format!("tiny_live_policy: {}", tiny_live_policy.reason)],
        );
    }
    if !tiny_live_guardrails.current_config_bounded_for_later_tiny_live_monitoring {
        return (
            TinyLiveActivationPlanVerdict::BlockedByGuardrailContract,
            format!(
                "tiny-live guardrail envelope is not bounded enough yet: {}",
                tiny_live_guardrails.reason
            ),
            vec![format!(
                "tiny_live_guardrails: {}",
                tiny_live_guardrails.reason
            )],
        );
    }
    if !overlay.complete {
        return (
            TinyLiveActivationPlanVerdict::ActivationOverlayIncomplete,
            overlay
                .blockers
                .first()
                .cloned()
                .unwrap_or_else(|| "activation overlay is incomplete".to_string()),
            overlay
                .blockers
                .iter()
                .map(|blocker| format!("activation_overlay: {blocker}"))
                .collect(),
        );
    }
    if !rollback_plan_complete {
        return (
            TinyLiveActivationPlanVerdict::RollbackPlanIncomplete,
            "rollback overlay could not be rendered explicitly".to_string(),
            vec!["rollback_plan: no explicit rollback delta could be generated".to_string()],
        );
    }
    if !service_restart_contract_complete {
        return (
            TinyLiveActivationPlanVerdict::ServiceRestartContractIncomplete,
            "service restart contract is incomplete for activation or rollback".to_string(),
            vec!["service_restart_contract: missing explicit service restart steps".to_string()],
        );
    }

    (
        TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows,
        "bounded tiny-live activation candidate is explicit; if the consolidated pre-activation gate is green at decision time, the future config delta and rollback delta are already prepared".to_string(),
        Vec::new(),
    )
}

fn build_overlay_plan(
    loaded_config: &AppConfig,
    _tiny_live_policy: &tiny_live_policy_audit::TinyLivePolicyAuditReport,
) -> OverlayBuildResult {
    let policy = &loaded_config.tiny_live_policy;
    let mut blockers = Vec::new();
    let mut drift_findings = Vec::new();
    let mut changes = Vec::new();
    let mut unchanged = Vec::new();

    let policy_allowed_routes = normalize_routes(&policy.allowed_routes);
    if policy_allowed_routes.is_empty() {
        blockers.push(
            "tiny_live_policy.allowed_routes is empty, so the activation candidate route set is undefined"
                .to_string(),
        );
    }

    let current_default_route = normalize_route(loaded_config.execution.default_route.as_str());
    let current_route_order = normalize_routes(&loaded_config.execution.submit_route_order);
    let candidate_route_order = derive_candidate_route_order(
        &policy_allowed_routes,
        &current_route_order,
        &mut drift_findings,
    );
    if candidate_route_order.is_empty() {
        blockers.push(
            "activation candidate route order is empty after applying the bounded tiny-live route set"
                .to_string(),
        );
    }

    let candidate_default_route = derive_candidate_default_route(
        &current_default_route,
        &policy_allowed_routes,
        &candidate_route_order,
    );
    if candidate_default_route.is_empty() {
        blockers.push(
            "activation candidate default route could not be derived from current config and tiny-live policy"
                .to_string(),
        );
    }

    let candidate_policy_echo_required =
        loaded_config.execution.submit_adapter_require_policy_echo || policy.require_policy_echo;
    let candidate_shadow_copy_notional = loaded_config
        .shadow
        .copy_notional_sol
        .min(policy.max_trade_notional_sol);
    let candidate_risk_max_position = loaded_config
        .risk
        .max_position_sol
        .min(policy.max_trade_notional_sol);
    let candidate_batch_size = loaded_config
        .execution
        .batch_size
        .min(policy.max_batch_size);
    let candidate_max_concurrent_positions = loaded_config
        .risk
        .max_concurrent_positions
        .min(policy.max_concurrent_positions);
    let candidate_daily_loss_limit_pct = loaded_config
        .risk
        .daily_loss_limit_pct
        .min(policy.max_daily_loss_limit_pct);
    let candidate_pretrade_fee_overhead_bps = loaded_config
        .execution
        .pretrade_max_fee_overhead_bps
        .min(policy.max_pretrade_fee_overhead_bps);
    let candidate_pretrade_priority_fee_lamports = loaded_config
        .execution
        .pretrade_max_priority_fee_lamports
        .min(policy.max_pretrade_priority_fee_lamports);

    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.enabled",
        json!(loaded_config.execution.enabled),
        json!(true),
        "future tiny-live activation toggle; this command does not apply it",
        "activation_toggle",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.mode",
        json!(normalize_mode(loaded_config.execution.mode.as_str())),
        json!(TARGET_EXECUTION_MODE),
        "tiny-live candidate stays on the accepted adapter_submit_confirm execution path",
        "target_mode",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.default_route",
        json!(current_default_route),
        json!(candidate_default_route.clone()),
        "default route must stay inside the bounded tiny-live allowed-route set",
        "bounded_route_contract",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.submit_allowed_routes",
        json!(normalize_routes(
            &loaded_config.execution.submit_allowed_routes
        )),
        json!(policy_allowed_routes.clone()),
        "tiny-live candidate route set comes from explicit tiny_live_policy.allowed_routes",
        "policy_allowed_routes",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.submit_route_order",
        json!(current_route_order.clone()),
        json!(candidate_route_order.clone()),
        "route order must be explicit for a restart-safe tiny-live candidate",
        "explicit_route_order",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.submit_adapter_require_policy_echo",
        json!(loaded_config.execution.submit_adapter_require_policy_echo),
        json!(candidate_policy_echo_required),
        "tiny-live candidate must preserve or strengthen policy-echo enforcement",
        "policy_echo_guard",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "shadow.copy_notional_sol",
        json!(loaded_config.shadow.copy_notional_sol),
        json!(candidate_shadow_copy_notional),
        "tiny-live notional cannot exceed the explicit bounded policy cap",
        "bounded_notional_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "risk.max_position_sol",
        json!(loaded_config.risk.max_position_sol),
        json!(candidate_risk_max_position),
        "tiny-live max position cannot exceed the bounded per-trade policy cap",
        "bounded_position_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.batch_size",
        json!(loaded_config.execution.batch_size),
        json!(candidate_batch_size),
        "tiny-live batch size cannot exceed the explicit bounded policy cap",
        "bounded_batch_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "risk.max_concurrent_positions",
        json!(loaded_config.risk.max_concurrent_positions),
        json!(candidate_max_concurrent_positions),
        "tiny-live concurrent positions cannot exceed the bounded policy cap",
        "bounded_concurrency_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "risk.daily_loss_limit_pct",
        json!(loaded_config.risk.daily_loss_limit_pct),
        json!(candidate_daily_loss_limit_pct),
        "tiny-live daily loss guardrail cannot exceed the bounded policy cap",
        "bounded_daily_loss_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.pretrade_max_fee_overhead_bps",
        json!(loaded_config.execution.pretrade_max_fee_overhead_bps),
        json!(candidate_pretrade_fee_overhead_bps),
        "tiny-live pretrade fee overhead must remain inside the bounded policy cap",
        "bounded_fee_overhead_cap",
    );
    push_change_or_unchanged(
        &mut changes,
        &mut unchanged,
        "execution.pretrade_max_priority_fee_lamports",
        json!(loaded_config.execution.pretrade_max_priority_fee_lamports),
        json!(candidate_pretrade_priority_fee_lamports),
        "tiny-live priority-fee envelope must remain inside the bounded policy cap",
        "bounded_priority_fee_cap",
    );

    let route_contract = build_route_envelope(
        loaded_config,
        &policy_allowed_routes,
        &mut blockers,
        &mut drift_findings,
        &mut changes,
        &mut unchanged,
    );

    add_explicitly_unchanged_field(
        &mut unchanged,
        "sqlite.path",
        json!(loaded_config.sqlite.path.clone()),
        "activation plan never changes the runtime DB path",
    );
    add_explicitly_unchanged_field(
        &mut unchanged,
        "execution.rpc_http_url",
        json!(loaded_config.execution.rpc_http_url.clone()),
        "activation plan reuses the already-audited execution RPC endpoint",
    );
    add_explicitly_unchanged_field(
        &mut unchanged,
        "execution.submit_adapter_http_url",
        json!(loaded_config.execution.submit_adapter_http_url.clone()),
        "activation plan reuses the already-audited production adapter endpoint",
    );
    add_explicitly_unchanged_field(
        &mut unchanged,
        "execution.execution_signer_pubkey",
        json!(loaded_config.execution.execution_signer_pubkey.clone()),
        "activation plan reuses the already-audited signer contract",
    );

    let complete = blockers.is_empty();
    let effective_contract = if complete {
        Some(EffectiveTinyLiveContract {
            execution_mode: TARGET_EXECUTION_MODE.to_string(),
            execution_enabled_target: true,
            default_route: candidate_default_route,
            allowed_routes: policy_allowed_routes,
            route_order: candidate_route_order,
            policy_echo_required: candidate_policy_echo_required,
            shadow_copy_notional_sol: candidate_shadow_copy_notional,
            risk_max_position_sol: candidate_risk_max_position,
            execution_batch_size: candidate_batch_size,
            risk_max_concurrent_positions: candidate_max_concurrent_positions,
            risk_daily_loss_limit_pct: candidate_daily_loss_limit_pct,
            pretrade_max_fee_overhead_bps: candidate_pretrade_fee_overhead_bps,
            pretrade_max_priority_fee_lamports: candidate_pretrade_priority_fee_lamports,
            route_envelope: route_contract,
        })
    } else {
        None
    };

    OverlayBuildResult {
        complete,
        blockers,
        drift_findings,
        activation_overlay_changes: changes,
        unchanged_fields: unchanged,
        effective_contract,
    }
}

fn build_route_envelope(
    loaded_config: &AppConfig,
    candidate_routes: &[String],
    blockers: &mut Vec<String>,
    drift_findings: &mut Vec<String>,
    changes: &mut Vec<ActivationPlanFieldDelta>,
    unchanged: &mut Vec<UnchangedFieldSummary>,
) -> Vec<RouteEnvelopeRow> {
    let mut candidate_slippage = BTreeMap::new();
    let mut candidate_tip = BTreeMap::new();
    let mut candidate_cu_price = BTreeMap::new();
    let mut candidate_cu_limit = BTreeMap::new();
    let mut route_rows = Vec::new();

    for route in candidate_routes {
        let current_cu_limit = lookup_route_u32(
            &loaded_config.execution.submit_route_compute_unit_limit,
            route,
        );
        let Some(current_cu_limit) = current_cu_limit else {
            blockers.push(format!(
                "execution.submit_route_compute_unit_limit has no explicit value for activation candidate route={route}"
            ));
            continue;
        };
        let Some(policy_slippage) = lookup_route_f64(
            &loaded_config.tiny_live_policy.max_route_slippage_bps,
            route,
        ) else {
            blockers.push(format!(
                "tiny_live_policy.max_route_slippage_bps is missing explicit cap for route={route}"
            ));
            continue;
        };
        let Some(policy_tip) = lookup_route_u64(
            &loaded_config.tiny_live_policy.max_route_tip_lamports,
            route,
        ) else {
            blockers.push(format!(
                "tiny_live_policy.max_route_tip_lamports is missing explicit cap for route={route}"
            ));
            continue;
        };
        let Some(policy_cu_price) = lookup_route_u64(
            &loaded_config
                .tiny_live_policy
                .max_route_compute_unit_price_micro_lamports,
            route,
        ) else {
            blockers.push(format!(
                "tiny_live_policy.max_route_compute_unit_price_micro_lamports is missing explicit cap for route={route}"
            ));
            continue;
        };

        let current_slippage = lookup_route_f64(
            &loaded_config.execution.submit_route_max_slippage_bps,
            route,
        )
        .unwrap_or(policy_slippage);
        let current_tip =
            lookup_route_u64(&loaded_config.execution.submit_route_tip_lamports, route)
                .unwrap_or(policy_tip);
        let current_cu_price = lookup_route_u64(
            &loaded_config
                .execution
                .submit_route_compute_unit_price_micro_lamports,
            route,
        )
        .unwrap_or(policy_cu_price);
        let effective_slippage = current_slippage.min(policy_slippage);
        let effective_tip = current_tip.min(policy_tip);
        let effective_cu_price = current_cu_price.min(policy_cu_price);

        if current_slippage != effective_slippage
            || current_tip != effective_tip
            || current_cu_price != effective_cu_price
        {
            drift_findings.push(format!(
                "route={route} requires tighter fee/slippage bounds for the tiny-live candidate"
            ));
        }

        candidate_slippage.insert(route.clone(), effective_slippage);
        candidate_tip.insert(route.clone(), effective_tip);
        candidate_cu_price.insert(route.clone(), effective_cu_price);
        candidate_cu_limit.insert(route.clone(), current_cu_limit);
        route_rows.push(RouteEnvelopeRow {
            route: route.clone(),
            compute_unit_limit: current_cu_limit,
            slippage_bps: effective_slippage,
            tip_lamports: effective_tip,
            compute_unit_price_micro_lamports: effective_cu_price,
        });
    }

    push_change_or_unchanged(
        changes,
        unchanged,
        "execution.submit_route_max_slippage_bps",
        json!(normalize_route_map_f64(
            &loaded_config.execution.submit_route_max_slippage_bps
        )),
        json!(candidate_slippage),
        "tiny-live route slippage caps are bounded by the explicit policy envelope",
        "per_route_slippage_cap",
    );
    push_change_or_unchanged(
        changes,
        unchanged,
        "execution.submit_route_tip_lamports",
        json!(normalize_route_map_u64(
            &loaded_config.execution.submit_route_tip_lamports
        )),
        json!(candidate_tip),
        "tiny-live route tips are bounded by the explicit policy envelope",
        "per_route_tip_cap",
    );
    push_change_or_unchanged(
        changes,
        unchanged,
        "execution.submit_route_compute_unit_price_micro_lamports",
        json!(normalize_route_map_u64(
            &loaded_config
                .execution
                .submit_route_compute_unit_price_micro_lamports
        )),
        json!(candidate_cu_price),
        "tiny-live route CU prices are bounded by the explicit policy envelope",
        "per_route_cu_price_cap",
    );
    push_change_or_unchanged(
        changes,
        unchanged,
        "execution.submit_route_compute_unit_limit",
        json!(normalize_route_map_u32(
            &loaded_config.execution.submit_route_compute_unit_limit
        )),
        json!(candidate_cu_limit),
        "tiny-live route CU limits are reused from the current execution contract",
        "current_route_compute_limit_reuse",
    );

    route_rows
}

fn build_service_restart_contract() -> ServiceRestartContract {
    ServiceRestartContract {
        complete: true,
        activation_restart_required: true,
        rollback_restart_required: true,
        activation_services: vec![TARGET_SERVICE_NAME.to_string()],
        rollback_services: vec![TARGET_SERVICE_NAME.to_string()],
        activation_steps: vec![
            "review that copybot_pre_activation_gate_report is green at decision time".to_string(),
            "apply only the generated activation overlay fields to /etc/solana-copy-bot/live.server.toml".to_string(),
            format!("sudo systemctl restart {TARGET_SERVICE_NAME}"),
            format!(
                "inspect journalctl -u {TARGET_SERVICE_NAME} and keep execution scope bounded to the generated tiny-live envelope"
            ),
        ],
        rollback_steps: vec![
            "apply the generated rollback delta to return execution config to the current safe state".to_string(),
            format!("sudo systemctl restart {TARGET_SERVICE_NAME}"),
            format!(
                "inspect journalctl -u {TARGET_SERVICE_NAME} and confirm execution.enabled is back to false"
            ),
        ],
    }
}

fn derive_candidate_route_order(
    policy_allowed_routes: &[String],
    current_route_order: &[String],
    drift_findings: &mut Vec<String>,
) -> Vec<String> {
    let mut filtered = current_route_order
        .iter()
        .filter(|route| {
            policy_allowed_routes
                .iter()
                .any(|allowed| allowed == *route)
        })
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        if !policy_allowed_routes.is_empty() {
            drift_findings.push(
                "execution.submit_route_order is implicit or out of policy scope; activation candidate makes it explicit from tiny_live_policy.allowed_routes"
                    .to_string(),
            );
        }
        filtered = policy_allowed_routes.to_vec();
    } else {
        for route in policy_allowed_routes {
            if !filtered.iter().any(|existing| existing == route) {
                filtered.push(route.clone());
            }
        }
    }
    dedupe_preserving_order(&filtered)
}

fn derive_candidate_default_route(
    current_default_route: &str,
    policy_allowed_routes: &[String],
    candidate_route_order: &[String],
) -> String {
    if policy_allowed_routes
        .iter()
        .any(|route| route == current_default_route)
    {
        current_default_route.to_string()
    } else {
        candidate_route_order.first().cloned().unwrap_or_default()
    }
}

fn push_change_or_unchanged(
    changes: &mut Vec<ActivationPlanFieldDelta>,
    unchanged: &mut Vec<UnchangedFieldSummary>,
    field: &str,
    current_value: Value,
    activation_value: Value,
    reason: &str,
    source: &str,
) {
    if current_value == activation_value {
        unchanged.push(UnchangedFieldSummary {
            field: field.to_string(),
            value: activation_value,
            reason: reason.to_string(),
        });
    } else {
        changes.push(ActivationPlanFieldDelta {
            field: field.to_string(),
            current_value: current_value.clone(),
            activation_value: activation_value.clone(),
            rollback_value: current_value,
            reason: reason.to_string(),
            source: source.to_string(),
        });
    }
}

fn add_explicitly_unchanged_field(
    unchanged: &mut Vec<UnchangedFieldSummary>,
    field: &str,
    value: Value,
    reason: &str,
) {
    unchanged.push(UnchangedFieldSummary {
        field: field.to_string(),
        value,
        reason: reason.to_string(),
    });
}

fn lookup_route_f64(map: &BTreeMap<String, f64>, route: &str) -> Option<f64> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn lookup_route_u64(map: &BTreeMap<String, u64>, route: &str) -> Option<u64> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn lookup_route_u32(map: &BTreeMap<String, u32>, route: &str) -> Option<u32> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn normalize_route(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_mode(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "<empty>".to_string()
    } else {
        normalized
    }
}

fn normalize_routes(values: &[String]) -> Vec<String> {
    dedupe_preserving_order(
        &values
            .iter()
            .map(|value| normalize_route(value))
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
    )
}

fn normalize_route_map_f64(map: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    map.iter()
        .map(|(key, value)| (normalize_route(key), *value))
        .collect()
}

fn normalize_route_map_u64(map: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    map.iter()
        .map(|(key, value)| (normalize_route(key), *value))
        .collect()
}

fn normalize_route_map_u32(map: &BTreeMap<String, u32>) -> BTreeMap<String, u32> {
    map.iter()
        .map(|(key, value)| (normalize_route(key), *value))
        .collect()
}

fn dedupe_preserving_order(values: &[String]) -> Vec<String> {
    let mut seen = Vec::new();
    for value in values {
        if !seen.iter().any(|existing| existing == value) {
            seen.push(value.clone());
        }
    }
    seen
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

pub(crate) fn render_human(report: &TinyLiveActivationPlanReport) -> String {
    [
        "event=copybot_tiny_live_activation_plan".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!(
            "output_path={}",
            report
                .output_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "execution_enabled_current={}",
            report.execution_enabled_current
        ),
        format!("current_execution_mode={}", report.current_execution_mode),
        format!("planning_safe_only={}", report.planning_safe_only),
        format!(
            "activation_permission_granted={}",
            report.activation_permission_granted
        ),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("blockers={}", report.blockers.join(" | ")),
        format!(
            "pre_activation_gate_verdict={}",
            report.pre_activation_gate.verdict
        ),
        format!(
            "pre_activation_gate_reason={}",
            report.pre_activation_gate.reason
        ),
        format!(
            "tiny_live_policy_verdict={}",
            report.tiny_live_policy.verdict
        ),
        format!("tiny_live_policy_reason={}", report.tiny_live_policy.reason),
        format!(
            "tiny_live_guardrail_verdict={}",
            report.tiny_live_guardrails.verdict
        ),
        format!(
            "tiny_live_guardrail_reason={}",
            report.tiny_live_guardrails.reason
        ),
        format!(
            "tiny_live_guardrails_bounded={}",
            report.tiny_live_guardrails.bounded
        ),
        format!(
            "tiny_live_guardrails_enabled={}",
            report.tiny_live_guardrails.tiny_live_guardrails_enabled
        ),
        format!(
            "tiny_live_guardrail_first_blocker={}",
            report
                .tiny_live_guardrails
                .first_blocker
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "activation_overlay_complete={}",
            report.activation_overlay_complete
        ),
        format!("rollback_plan_complete={}", report.rollback_plan_complete),
        format!(
            "service_restart_contract_complete={}",
            report.service_restart_contract_complete
        ),
        format!(
            "activation_overlay_change_count={}",
            report.activation_overlay_change_count
        ),
        format!("drift_findings={}", report.drift_findings.join(" | ")),
        format!(
            "rollback_trigger_summary={}",
            report
                .tiny_live_guardrails
                .rollback_triggers
                .iter()
                .map(render_guardrail_trigger_summary)
                .collect::<Vec<_>>()
                .join(" ; ")
        ),
        format!(
            "activation_overlay_changes={}",
            report
                .activation_overlay_changes
                .iter()
                .map(|change| format!(
                    "{}:{}->{}",
                    change.field, change.current_value, change.activation_value
                ))
                .collect::<Vec<_>>()
                .join(" ; ")
        ),
        format!(
            "rollback_overlay_changes={}",
            report
                .activation_overlay_changes
                .iter()
                .map(|change| format!(
                    "{}:{}->{}",
                    change.field, change.activation_value, change.rollback_value
                ))
                .collect::<Vec<_>>()
                .join(" ; ")
        ),
        format!(
            "unchanged_fields={}",
            report
                .unchanged_fields
                .iter()
                .map(|field| field.field.clone())
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!(
            "activation_services={}",
            report
                .service_restart_contract
                .activation_services
                .join(",")
        ),
        format!(
            "rollback_services={}",
            report.service_restart_contract.rollback_services.join(",")
        ),
    ]
    .join("\n")
}

fn render_guardrail_trigger_summary(
    trigger: &tiny_live_guardrail_audit::RollbackTriggerSummary,
) -> String {
    format!(
        "{}(kind={},rate_pct={},window_seconds={},sol={},count={},evaluation_window_seconds={},action={})",
        trigger.trigger,
        trigger.threshold_kind,
        trigger
            .threshold_rate_pct
            .map(|value| format!("{value:.4}"))
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_sol
            .map(|value| format!("{value:.6}"))
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .evaluation_window_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger.action,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::AppConfig;
    use copybot_discovery::wallet_freshness_audit::WalletFreshnessHistoryVerdict;

    #[test]
    fn blocked_pre_activation_gate_blocks_plan() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage3,
                "stage3 blocked",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::BlockedByPreActivationGate
        );
    }

    #[test]
    fn green_gate_and_bounded_policy_produce_ready_plan() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
        );
        assert!(report.activation_overlay_complete);
        assert!(report
            .activation_overlay_changes
            .iter()
            .any(|change| change.field == "execution.enabled"));
    }

    #[test]
    fn missing_overlay_fields_yield_activation_overlay_incomplete() {
        let mut config = bounded_config();
        config
            .execution
            .submit_route_compute_unit_limit
            .remove("jito");

        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::ActivationOverlayIncomplete
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("submit_route_compute_unit_limit")));
    }

    #[test]
    fn rollback_contract_is_rendered_explicitly() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert!(report.rollback_plan_complete);
        assert!(report.service_restart_contract.rollback_restart_required);
        assert!(report
            .service_restart_contract
            .rollback_steps
            .iter()
            .any(|step| step.contains("execution.enabled is back to false")));
    }

    #[test]
    fn tool_does_not_mutate_execution_enabled_in_source_config() {
        let config = bounded_config();
        assert!(!config.execution.enabled);
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert!(!config.execution.enabled);
        assert!(!report.execution_enabled_current);
    }

    #[test]
    fn no_restore_or_recovery_truth_is_needed_for_plan() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                WalletFreshnessHistoryVerdict::ValidatedCurrent.as_str(),
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
        );
        assert!(report.planning_safe_only);
        assert!(!report.activation_permission_granted);
    }

    #[test]
    fn policy_non_green_blocks_before_guardrails() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            policy_report(
                tiny_live_policy_audit::TinyLivePolicyVerdict::TinyLivePolicyTooOpen,
                "policy too open",
                false,
            ),
            guardrail_report(
                tiny_live_guardrail_audit::TinyLiveGuardrailVerdict::TinyLiveGuardrailsTooOpen,
                "guardrails too open",
                false,
            ),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::BlockedByPolicyContract
        );
    }

    #[test]
    fn guardrail_non_green_with_gate_and_policy_green_blocks_plan() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            guardrail_report(
                tiny_live_guardrail_audit::TinyLiveGuardrailVerdict::TinyLiveGuardrailsTooOpen,
                "connectivity rollback window too wide",
                false,
            ),
        );

        assert_eq!(
            report.verdict,
            TinyLiveActivationPlanVerdict::BlockedByGuardrailContract
        );
        assert!(report.reason.contains("guardrail"));
    }

    #[test]
    fn ready_plan_includes_guardrail_rollback_trigger_summary() {
        let config = bounded_config();
        let report = build_activation_plan_report(
            &test_config(),
            &config,
            pre_activation_report(
                pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
                "green",
            ),
            bounded_policy_report(),
            bounded_guardrail_report(),
        );

        assert!(report.tiny_live_guardrails.bounded);
        assert!(!report.tiny_live_guardrails.rollback_triggers.is_empty());
        assert!(report
            .tiny_live_guardrails
            .rollback_triggers
            .iter()
            .any(|trigger| trigger.trigger == "consecutive_hard_failures"));
    }

    fn test_config() -> Config {
        Config {
            config_path: PathBuf::from("/tmp/live.server.toml"),
            json: false,
            output_path: None,
            now: ts("2026-03-26T12:00:00Z"),
            stage3_limit: DEFAULT_HISTORY_CAPTURE_LIMIT,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: DEFAULT_REHEARSAL_HISTORY_LIMIT,
            rehearsal_recent_horizon_seconds: DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
            min_recent_acceptable_rehearsals: DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
        }
    }

    fn pre_activation_report(
        verdict: pre_activation_gate_report::PreActivationGateVerdict,
        reason: &str,
    ) -> pre_activation_gate_report::PreActivationGateReport {
        pre_activation_gate_report::PreActivationGateReport {
            generated_at: ts("2026-03-26T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            db_path: "/tmp/live_runtime.db".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            activation_permission_granted: false,
            stage3_is_primary_gate: true,
            verdict,
            reason: reason.to_string(),
            blockers: Vec::new(),
            stage3: pre_activation_gate_report::Stage3GateSummary {
                verdict: "validated_current".to_string(),
                reason: "validated".to_string(),
                stage3_green: true,
                captures_loaded: 3,
                captures_within_recent_horizon: 3,
                recent_horizon_seconds: 3_600,
                latest_capture_age_seconds: Some(60),
                stale_captures_excluded_from_verdict: false,
                exact_published_current_match_count: 3,
                exact_active_current_match_count: 3,
                rotation_evidence_capture_count: 3,
                shadow_signal_present_capture_count: 3,
            },
            stage4_readiness: pre_activation_gate_report::Stage4ReadinessSummary {
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
            stage4_dry_run_history: pre_activation_gate_report::DryRunHistorySummary {
                verdict:
                    pre_activation_gate_report::DryRunHistoryVerdict::SufficientRecentRehearsalEvidence,
                reason: "sufficient".to_string(),
                records_loaded: 2,
                recent_rehearsals_within_horizon: 2,
                recent_horizon_seconds: 3_600,
                latest_rehearsal_age_seconds: Some(120),
                stale_rehearsals_excluded_from_verdict: false,
                stale_rehearsals_excluded_count: 0,
                acceptable_recent_rehearsal_count: 2,
                recent_hard_blocker_count: 0,
                latest_recent_verdict: Some("rehearsal_green".to_string()),
                verdict_counts: BTreeMap::from([("rehearsal_green".to_string(), 2)]),
            },
            tiny_live_policy: pre_activation_gate_report::TinyLivePolicySummary {
                verdict: "tiny_live_policy_bounded".to_string(),
                reason: "bounded".to_string(),
                tiny_live_policy_bounded: true,
                tiny_live_policy_enabled: true,
                blocker_count: 0,
                first_blocker: None,
                warnings_count: 1,
                mode_compatible: true,
                execution_policy_contract_valid: true,
                execution_route_contract_valid: true,
            },
        }
    }

    fn bounded_policy_report() -> tiny_live_policy_audit::TinyLivePolicyAuditReport {
        policy_report(
            tiny_live_policy_audit::TinyLivePolicyVerdict::TinyLivePolicyBounded,
            "bounded",
            true,
        )
    }

    fn policy_report(
        verdict: tiny_live_policy_audit::TinyLivePolicyVerdict,
        reason: &str,
        bounded: bool,
    ) -> tiny_live_policy_audit::TinyLivePolicyAuditReport {
        tiny_live_policy_audit::TinyLivePolicyAuditReport {
            generated_at: ts("2026-03-26T12:00:00Z"),
            config_path: "/tmp/live.server.toml".to_string(),
            execution_enabled: false,
            planning_safe_only: true,
            stage3_gate_not_evaluated: true,
            mode: "adapter_submit_confirm".to_string(),
            mode_compatible: true,
            execution_policy_contract_valid: true,
            execution_route_contract_valid: true,
            current_config_bounded_for_later_tiny_live_discussion: bounded,
            suitable_only_for_paper_or_dry_run: !bounded,
            verdict,
            reason: reason.to_string(),
            blockers: if bounded {
                Vec::new()
            } else {
                vec![reason.to_string()]
            },
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
            policy_max_pretrade_fee_overhead_bps: 1_000,
            current_pretrade_max_priority_fee_lamports: 1_500,
            policy_max_pretrade_priority_fee_lamports: 2_000,
            current_policy_echo_required: true,
            policy_echo_required_for_tiny_live: true,
            route_policy_rows: vec![tiny_live_policy_audit::RoutePolicyAuditRow {
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
            }],
        }
    }

    fn bounded_guardrail_report() -> tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport {
        guardrail_report(
            tiny_live_guardrail_audit::TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded,
            "guardrails bounded",
            true,
        )
    }

    fn guardrail_report(
        verdict: tiny_live_guardrail_audit::TinyLiveGuardrailVerdict,
        reason: &str,
        bounded: bool,
    ) -> tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport {
        tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport {
            generated_at: ts("2026-03-26T12:00:00Z"),
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
            monitoring_contract_complete: bounded,
            rollback_contract_complete: bounded,
            current_config_bounded_for_later_tiny_live_monitoring: bounded,
            verdict,
            reason: reason.to_string(),
            blockers: if bounded {
                Vec::new()
            } else {
                vec![reason.to_string()]
            },
            warnings: vec!["execution.enabled=false".to_string()],
            evaluation_window_seconds: 900,
            max_execution_error_rate_pct: 5.0,
            max_adapter_contract_failure_rate_pct: 1.0,
            max_policy_echo_mismatch_rate_pct: 1.0,
            max_fee_or_slippage_breach_rate_pct: 5.0,
            max_connectivity_degraded_window_seconds: 120,
            max_daily_realized_loss_sol: 0.05,
            max_consecutive_hard_failures: 3,
            reference_envelope: tiny_live_guardrail_audit::GuardrailReferenceEnvelope {
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
                tiny_live_guardrail_audit::RollbackTriggerSummary {
                    trigger: "execution_error_rate".to_string(),
                    threshold_kind: "rate_pct".to_string(),
                    threshold_rate_pct: Some(5.0),
                    threshold_seconds: None,
                    threshold_sol: None,
                    threshold_count: None,
                    evaluation_window_seconds: Some(900),
                    action: "mandatory_rollback".to_string(),
                },
                tiny_live_guardrail_audit::RollbackTriggerSummary {
                    trigger: "consecutive_hard_failures".to_string(),
                    threshold_kind: "count".to_string(),
                    threshold_rate_pct: None,
                    threshold_seconds: None,
                    threshold_sol: None,
                    threshold_count: Some(3),
                    evaluation_window_seconds: None,
                    action: "mandatory_rollback".to_string(),
                },
            ],
        }
    }

    fn bounded_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "prod-live".to_string();
        config.execution.enabled = false;
        config.execution.mode = "adapter_submit_confirm".to_string();
        config.execution.default_route = "jito".to_string();
        config.execution.submit_allowed_routes = vec!["jito".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 40.0)].into_iter().collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000)].into_iter().collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config.execution.submit_adapter_require_policy_echo = true;
        config.execution.pretrade_max_fee_overhead_bps = 800;
        config.execution.pretrade_max_priority_fee_lamports = 1_500;
        config.execution.rpc_http_url = "https://rpc.example".to_string();
        config.execution.submit_adapter_http_url = "http://127.0.0.1:8080/submit".to_string();
        config.execution.execution_signer_pubkey = "11111111111111111111111111111111".to_string();
        config.shadow.copy_notional_sol = 0.05;
        config.risk.max_position_sol = 0.05;
        config.risk.max_concurrent_positions = 1;
        config.risk.daily_loss_limit_pct = 0.75;
        config.execution.batch_size = 1;
        config.tiny_live_policy.enabled = true;
        config.tiny_live_policy.max_trade_notional_sol = 0.05;
        config.tiny_live_policy.max_batch_size = 1;
        config.tiny_live_policy.max_concurrent_positions = 1;
        config.tiny_live_policy.max_daily_loss_limit_pct = 1.0;
        config.tiny_live_policy.allowed_routes = vec!["jito".to_string()];
        config.tiny_live_policy.require_policy_echo = true;
        config.tiny_live_policy.max_pretrade_fee_overhead_bps = 1_000;
        config.tiny_live_policy.max_pretrade_priority_fee_lamports = 2_000;
        config.tiny_live_policy.max_route_slippage_bps =
            [("jito".to_string(), 50.0)].into_iter().collect();
        config.tiny_live_policy.max_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("valid ts")
            .with_timezone(&Utc)
    }
}
