use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

#[allow(dead_code)]
#[path = "copybot_devnet_dress_rehearsal.rs"]
mod devnet_dress_rehearsal;
#[allow(dead_code)]
#[path = "copybot_devnet_readiness_report.rs"]
mod devnet_readiness_report;
#[allow(dead_code)]
#[path = "copybot_pre_activation_gate_report.rs"]
mod pre_activation_gate_report;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_plan.rs"]
mod tiny_live_activation_plan;
#[allow(dead_code)]
#[path = "copybot_tiny_live_guardrail_audit.rs"]
pub(crate) mod tiny_live_guardrail_audit;

const USAGE: &str = "usage: copybot_activation_checklist_report --config <prod-path> --non-prod-config <path> [--json] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>] [--non-prod-limit <count>] [--non-prod-dress-recent-horizon-seconds <seconds>] [--non-prod-activation-recent-horizon-seconds <seconds>] [--non-prod-min-recent-green-dress <count>] [--non-prod-min-recent-green-activation <count>]";
#[allow(dead_code)]
pub(crate) const DEFAULT_REHEARSAL_HISTORY_LIMIT: usize =
    tiny_live_activation_plan::DEFAULT_REHEARSAL_HISTORY_LIMIT;
#[allow(dead_code)]
pub(crate) const DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS: u64 =
    tiny_live_activation_plan::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS: usize =
    tiny_live_activation_plan::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_HISTORY_LIMIT: usize =
    devnet_readiness_report::DEFAULT_HISTORY_LIMIT;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS: u64 =
    devnet_readiness_report::DEFAULT_DRESS_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS: u64 =
    devnet_readiness_report::DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS: usize =
    devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_DRESS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION: usize =
    devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_ACTIVATION;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation checklist report")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) non_prod_config_path: PathBuf,
    pub(crate) json: bool,
    pub(crate) now: DateTime<Utc>,
    pub(crate) stage3_limit: usize,
    pub(crate) stage3_recent_horizon_seconds: Option<u64>,
    pub(crate) rehearsal_limit: usize,
    pub(crate) rehearsal_recent_horizon_seconds: u64,
    pub(crate) min_recent_acceptable_rehearsals: usize,
    pub(crate) non_prod_limit: usize,
    pub(crate) non_prod_dress_recent_horizon_seconds: u64,
    pub(crate) non_prod_activation_recent_horizon_seconds: u64,
    pub(crate) non_prod_min_recent_green_dress: usize,
    pub(crate) non_prod_min_recent_green_activation: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActivationChecklistVerdict {
    ActivationChecklistBlockedByProdStage3,
    ActivationChecklistBlockedByProdGate,
    ActivationChecklistBlockedByLaunchDossier,
    ActivationChecklistBlockedByNonProdReadiness,
    ActivationChecklistDiscussionReadyButNotAuthorized,
    ActivationChecklistRefusedForProdProfileMismatch,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ProdPreActivationGateSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) planning_green: bool,
    pub(crate) blocked_by_stage3: bool,
    pub(crate) stage3_verdict: String,
    pub(crate) stage3_reason: String,
    pub(crate) stage3_captures_within_recent_horizon: usize,
    pub(crate) stage3_latest_capture_age_seconds: Option<u64>,
    pub(crate) stage4_readiness_verdict: String,
    pub(crate) stage4_rehearsal_history_verdict: String,
    pub(crate) tiny_live_policy_verdict: String,
    pub(crate) blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LaunchDossierSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) ready_when_stage_gate_allows: bool,
    pub(crate) activation_overlay_complete: bool,
    pub(crate) rollback_plan_complete: bool,
    pub(crate) service_restart_contract_complete: bool,
    pub(crate) activation_overlay_change_count: usize,
    pub(crate) rollback_overlay_change_count: usize,
    pub(crate) drift_finding_count: usize,
    pub(crate) blocker_count: usize,
    pub(crate) first_blocker: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GuardrailSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) bounded: bool,
    pub(crate) enabled: bool,
    pub(crate) blocker_count: usize,
    pub(crate) first_blocker: Option<String>,
    pub(crate) rollback_trigger_count: usize,
    pub(crate) rollback_triggers: Vec<tiny_live_guardrail_audit::RollbackTriggerSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NonProdReadinessSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) green: bool,
    pub(crate) prod_profile_refused: bool,
    pub(crate) config_env: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) dress_latest_record_age_seconds: Option<u64>,
    pub(crate) dress_recent_green_count: usize,
    pub(crate) activation_latest_record_age_seconds: Option<u64>,
    pub(crate) activation_recent_green_count: usize,
    pub(crate) activation_recent_rollback_success_count: usize,
    pub(crate) activation_recent_internal_consistency_count: usize,
    pub(crate) stale_evidence_excluded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ActivationChecklistReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) prod_config_path: String,
    pub(crate) non_prod_config_path: String,
    pub(crate) prod_config_env: String,
    pub(crate) non_prod_config_env: String,
    pub(crate) execution_enabled: bool,
    pub(crate) planning_safe_only: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) discussion_ready_only: bool,
    pub(crate) prod_stage3_remains_hard_gate: bool,
    pub(crate) non_prod_evidence_is_secondary: bool,
    pub(crate) verdict: ActivationChecklistVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) prod_pre_activation_gate: ProdPreActivationGateSummary,
    pub(crate) launch_dossier: LaunchDossierSummary,
    pub(crate) tiny_live_guardrails: GuardrailSummary,
    pub(crate) non_prod_readiness: NonProdReadinessSummary,
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
    let mut non_prod_config_path: Option<PathBuf> = None;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit = tiny_live_activation_plan::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds =
        tiny_live_activation_plan::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        tiny_live_activation_plan::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut non_prod_limit = devnet_readiness_report::DEFAULT_HISTORY_LIMIT;
    let mut non_prod_dress_recent_horizon_seconds =
        devnet_readiness_report::DEFAULT_DRESS_RECENT_HORIZON_SECONDS;
    let mut non_prod_activation_recent_horizon_seconds =
        devnet_readiness_report::DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut non_prod_min_recent_green_dress =
        devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_DRESS;
    let mut non_prod_min_recent_green_activation =
        devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_ACTIVATION;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--non-prod-config" => {
                non_prod_config_path = Some(PathBuf::from(parse_string_arg(
                    "--non-prod-config",
                    args.next(),
                )?))
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
            "--non-prod-limit" => {
                non_prod_limit = parse_usize_arg("--non-prod-limit", args.next())?
            }
            "--non-prod-dress-recent-horizon-seconds" => {
                non_prod_dress_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-dress-recent-horizon-seconds", args.next())?
            }
            "--non-prod-activation-recent-horizon-seconds" => {
                non_prod_activation_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-activation-recent-horizon-seconds", args.next())?
            }
            "--non-prod-min-recent-green-dress" => {
                non_prod_min_recent_green_dress =
                    parse_usize_arg("--non-prod-min-recent-green-dress", args.next())?
            }
            "--non-prod-min-recent-green-activation" => {
                non_prod_min_recent_green_activation =
                    parse_usize_arg("--non-prod-min-recent-green-activation", args.next())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        non_prod_config_path: non_prod_config_path
            .ok_or_else(|| anyhow!("missing required --non-prod-config"))?,
        json,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
        non_prod_limit: non_prod_limit.max(1),
        non_prod_dress_recent_horizon_seconds: non_prod_dress_recent_horizon_seconds.max(1),
        non_prod_activation_recent_horizon_seconds: non_prod_activation_recent_horizon_seconds
            .max(1),
        non_prod_min_recent_green_dress: non_prod_min_recent_green_dress.max(1),
        non_prod_min_recent_green_activation: non_prod_min_recent_green_activation.max(1),
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
    let report = evaluate_activation_checklist_report(&config).await?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing activation checklist report json")
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) async fn evaluate_activation_checklist_report(
    config: &Config,
) -> Result<ActivationChecklistReport> {
    let prod_loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    if !devnet_dress_rehearsal::is_production_like_env(prod_loaded_config.system.env.as_str()) {
        return Ok(build_profile_mismatch_report(
            config,
            prod_loaded_config.system.env.as_str(),
            "production-facing activation checklist requires a production-like --config profile",
        ));
    }

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
    let tiny_live_guardrails = tiny_live_guardrail_audit::evaluate_tiny_live_guardrails(
        &config.config_path,
        &prod_loaded_config,
    )?;
    let activation_plan = tiny_live_activation_plan::evaluate_tiny_live_activation_plan(
        &tiny_live_activation_plan::Config {
            config_path: config.config_path.clone(),
            json: false,
            output_path: None,
            now: config.now,
            stage3_limit: config.stage3_limit,
            stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
            rehearsal_limit: config.rehearsal_limit,
            rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
            min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
        },
        &prod_loaded_config,
    )
    .await?;
    let non_prod_evaluation = devnet_readiness_report::evaluate_devnet_readiness_report(
        &devnet_readiness_report::Config {
            config_path: config.non_prod_config_path.clone(),
            limit: config.non_prod_limit,
            dress_recent_horizon_seconds: config.non_prod_dress_recent_horizon_seconds,
            activation_recent_horizon_seconds: config.non_prod_activation_recent_horizon_seconds,
            min_recent_green_dress: config.non_prod_min_recent_green_dress,
            min_recent_green_activation: config.non_prod_min_recent_green_activation,
            json: false,
        },
        config.now,
    )?;

    let prod_gate_summary = summarize_prod_gate(&pre_activation_gate);
    let launch_dossier_summary = summarize_launch_dossier(&activation_plan);
    let guardrail_summary = summarize_guardrails(&tiny_live_guardrails);
    let non_prod_summary = summarize_non_prod(non_prod_evaluation);

    Ok(compose_report(
        config,
        prod_loaded_config.system.env.as_str(),
        prod_loaded_config.execution.enabled,
        prod_gate_summary,
        launch_dossier_summary,
        guardrail_summary,
        non_prod_summary,
    ))
}

fn summarize_prod_gate(
    report: &pre_activation_gate_report::PreActivationGateReport,
) -> ProdPreActivationGateSummary {
    ProdPreActivationGateSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        planning_green: report.verdict
            == pre_activation_gate_report::PreActivationGateVerdict::PreActivationGatesGreen,
        blocked_by_stage3: matches!(
            report.verdict,
            pre_activation_gate_report::PreActivationGateVerdict::BlockedByStage3
                | pre_activation_gate_report::PreActivationGateVerdict::InsufficientRecentEvidence
        ),
        stage3_verdict: report.stage3.verdict.clone(),
        stage3_reason: report.stage3.reason.clone(),
        stage3_captures_within_recent_horizon: report.stage3.captures_within_recent_horizon,
        stage3_latest_capture_age_seconds: report.stage3.latest_capture_age_seconds,
        stage4_readiness_verdict: report.stage4_readiness.verdict.clone(),
        stage4_rehearsal_history_verdict: serialize_enum(&report.stage4_dry_run_history.verdict),
        tiny_live_policy_verdict: report.tiny_live_policy.verdict.clone(),
        blockers: report.blockers.clone(),
    }
}

fn summarize_launch_dossier(
    report: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> LaunchDossierSummary {
    LaunchDossierSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        ready_when_stage_gate_allows: report.verdict
            == tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows,
        activation_overlay_complete: report.activation_overlay_complete,
        rollback_plan_complete: report.rollback_plan_complete,
        service_restart_contract_complete: report.service_restart_contract_complete,
        activation_overlay_change_count: report.activation_overlay_change_count,
        rollback_overlay_change_count: report.rollback_overlay_change_count,
        drift_finding_count: report.drift_findings.len(),
        blocker_count: report.blockers.len(),
        first_blocker: report.blockers.first().cloned(),
    }
}

fn summarize_guardrails(
    report: &tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
) -> GuardrailSummary {
    GuardrailSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        bounded: report.current_config_bounded_for_later_tiny_live_monitoring,
        enabled: report.tiny_live_guardrails_enabled,
        blocker_count: report.blockers.len(),
        first_blocker: report.blockers.first().cloned(),
        rollback_trigger_count: report.rollback_triggers.len(),
        rollback_triggers: report.rollback_triggers.clone(),
    }
}

fn summarize_non_prod(
    evaluation: devnet_readiness_report::DevnetReadinessEvaluation,
) -> NonProdReadinessSummary {
    match evaluation {
        devnet_readiness_report::DevnetReadinessEvaluation::Report(report) => {
            NonProdReadinessSummary {
                verdict: serialize_enum(&report.verdict),
                reason: report.reason,
                green: report.verdict
                    == devnet_readiness_report::DevnetReadinessVerdict::DevnetReadinessGreen,
                prod_profile_refused: report.prod_profile_refused,
                config_env: report.config_env,
                blockers: report.blockers,
                warnings: report.warnings,
                dress_latest_record_age_seconds: report
                    .dress_rehearsal_history
                    .latest_record_age_seconds,
                dress_recent_green_count: report.dress_rehearsal_history.recent_green_count,
                activation_latest_record_age_seconds: report
                    .activation_drill_history
                    .latest_record_age_seconds,
                activation_recent_green_count: report
                    .activation_drill_history
                    .recent_green_count,
                activation_recent_rollback_success_count: report
                    .activation_drill_history
                    .recent_rollback_success_count,
                activation_recent_internal_consistency_count: report
                    .activation_drill_history
                    .recent_internal_consistency_count,
                stale_evidence_excluded: report
                    .dress_rehearsal_history
                    .stale_records_excluded_from_verdict
                    || report
                        .activation_drill_history
                        .stale_records_excluded_from_verdict,
            }
        }
        devnet_readiness_report::DevnetReadinessEvaluation::Refusal(report) => {
            NonProdReadinessSummary {
                verdict: serialize_enum(&report.verdict),
                reason: report.reason,
                green: false,
                prod_profile_refused: true,
                config_env: report.config_env,
                blockers: vec!["non-prod readiness config was refused as production-like".to_string()],
                warnings: vec![
                    "non-prod readiness evidence remains secondary and must come from a non-production profile"
                        .to_string(),
                ],
                dress_latest_record_age_seconds: None,
                dress_recent_green_count: 0,
                activation_latest_record_age_seconds: None,
                activation_recent_green_count: 0,
                activation_recent_rollback_success_count: 0,
                activation_recent_internal_consistency_count: 0,
                stale_evidence_excluded: false,
            }
        }
    }
}

fn compose_report(
    config: &Config,
    prod_config_env: &str,
    execution_enabled: bool,
    prod_gate: ProdPreActivationGateSummary,
    launch_dossier: LaunchDossierSummary,
    guardrails: GuardrailSummary,
    non_prod_readiness: NonProdReadinessSummary,
) -> ActivationChecklistReport {
    let (verdict, reason, mut blockers) = derive_top_level_verdict(
        &prod_gate,
        &launch_dossier,
        &guardrails,
        &non_prod_readiness,
    );
    if !execution_enabled {
        blockers.push(
            "execution.enabled=false remains in force; this checklist is discussion-ready only"
                .to_string(),
        );
    }
    blockers.sort();
    blockers.dedup();
    let warnings = vec![
        "discussion-ready still does not authorize production activation".to_string(),
        "non-prod evidence remains secondary to the production Stage 3 gate".to_string(),
    ];

    ActivationChecklistReport {
        generated_at: config.now,
        prod_config_path: config.config_path.display().to_string(),
        non_prod_config_path: config.non_prod_config_path.display().to_string(),
        prod_config_env: prod_config_env.to_string(),
        non_prod_config_env: non_prod_readiness.config_env.clone(),
        execution_enabled,
        planning_safe_only: true,
        activation_authorized: false,
        discussion_ready_only: verdict
            == ActivationChecklistVerdict::ActivationChecklistDiscussionReadyButNotAuthorized,
        prod_stage3_remains_hard_gate: true,
        non_prod_evidence_is_secondary: true,
        verdict,
        reason,
        blockers,
        warnings,
        prod_pre_activation_gate: prod_gate,
        launch_dossier,
        tiny_live_guardrails: guardrails,
        non_prod_readiness,
    }
}

fn build_profile_mismatch_report(
    config: &Config,
    prod_config_env: &str,
    reason: &str,
) -> ActivationChecklistReport {
    compose_report(
        config,
        prod_config_env,
        false,
        ProdPreActivationGateSummary {
            verdict: "not_evaluated".to_string(),
            reason: "prod profile mismatch prevented evaluation".to_string(),
            planning_green: false,
            blocked_by_stage3: false,
            stage3_verdict: "not_evaluated".to_string(),
            stage3_reason: "not_evaluated".to_string(),
            stage3_captures_within_recent_horizon: 0,
            stage3_latest_capture_age_seconds: None,
            stage4_readiness_verdict: "not_evaluated".to_string(),
            stage4_rehearsal_history_verdict: "not_evaluated".to_string(),
            tiny_live_policy_verdict: "not_evaluated".to_string(),
            blockers: vec![reason.to_string()],
        },
        LaunchDossierSummary {
            verdict: "not_evaluated".to_string(),
            reason: "launch dossier was not evaluated because the prod profile did not match"
                .to_string(),
            ready_when_stage_gate_allows: false,
            activation_overlay_complete: false,
            rollback_plan_complete: false,
            service_restart_contract_complete: false,
            activation_overlay_change_count: 0,
            rollback_overlay_change_count: 0,
            drift_finding_count: 0,
            blocker_count: 1,
            first_blocker: Some(reason.to_string()),
        },
        GuardrailSummary {
            verdict: "not_evaluated".to_string(),
            reason: "guardrails were not evaluated because the prod profile did not match"
                .to_string(),
            bounded: false,
            enabled: false,
            blocker_count: 1,
            first_blocker: Some(reason.to_string()),
            rollback_trigger_count: 0,
            rollback_triggers: Vec::new(),
        },
        NonProdReadinessSummary {
            verdict: "not_evaluated".to_string(),
            reason: "non-prod readiness was not evaluated because the prod profile did not match"
                .to_string(),
            green: false,
            prod_profile_refused: false,
            config_env: "not_evaluated".to_string(),
            blockers: vec![reason.to_string()],
            warnings: Vec::new(),
            dress_latest_record_age_seconds: None,
            dress_recent_green_count: 0,
            activation_latest_record_age_seconds: None,
            activation_recent_green_count: 0,
            activation_recent_rollback_success_count: 0,
            activation_recent_internal_consistency_count: 0,
            stale_evidence_excluded: false,
        },
    )
}

fn derive_top_level_verdict(
    prod_gate: &ProdPreActivationGateSummary,
    launch_dossier: &LaunchDossierSummary,
    guardrails: &GuardrailSummary,
    non_prod_readiness: &NonProdReadinessSummary,
) -> (ActivationChecklistVerdict, String, Vec<String>) {
    if prod_gate.verdict == "not_evaluated" {
        return (
            ActivationChecklistVerdict::ActivationChecklistRefusedForProdProfileMismatch,
            prod_gate.reason.clone(),
            vec![format!("prod_config: {}", prod_gate.reason)],
        );
    }
    if non_prod_readiness.prod_profile_refused {
        return (
            ActivationChecklistVerdict::ActivationChecklistRefusedForProdProfileMismatch,
            format!(
                "non-prod readiness refused the supplied profile: {}",
                non_prod_readiness.reason
            ),
            vec![format!("non_prod_readiness: {}", non_prod_readiness.reason)],
        );
    }
    if prod_gate.blocked_by_stage3 {
        return (
            ActivationChecklistVerdict::ActivationChecklistBlockedByProdStage3,
            format!(
                "production Stage 3 remains the hard gate and is still blocked: {}",
                prod_gate.reason
            ),
            vec![format!("prod_pre_activation_gate: {}", prod_gate.reason)],
        );
    }
    if !prod_gate.planning_green {
        return (
            ActivationChecklistVerdict::ActivationChecklistBlockedByProdGate,
            format!(
                "production pre-activation gate is not green yet: {}",
                prod_gate.reason
            ),
            vec![format!("prod_pre_activation_gate: {}", prod_gate.reason)],
        );
    }
    if !launch_dossier.ready_when_stage_gate_allows || !guardrails.bounded {
        let reason = if !launch_dossier.ready_when_stage_gate_allows {
            launch_dossier.reason.clone()
        } else {
            guardrails.reason.clone()
        };
        return (
            ActivationChecklistVerdict::ActivationChecklistBlockedByLaunchDossier,
            format!(
                "bounded launch dossier is not ready enough for later discussion: {}",
                reason
            ),
            vec![format!("launch_dossier: {}", reason)],
        );
    }
    if !non_prod_readiness.green {
        return (
            ActivationChecklistVerdict::ActivationChecklistBlockedByNonProdReadiness,
            format!(
                "non-production rehearsal/drill evidence is not green enough yet: {}",
                non_prod_readiness.reason
            ),
            vec![format!("non_prod_readiness: {}", non_prod_readiness.reason)],
        );
    }

    (
        ActivationChecklistVerdict::ActivationChecklistDiscussionReadyButNotAuthorized,
        "production pre-activation evidence, bounded launch dossier, bounded guardrails, and recent non-production readiness evidence are all green enough for later manual activation discussion; production activation is still not authorized by this report".to_string(),
        Vec::new(),
    )
}

pub(crate) fn render_human(report: &ActivationChecklistReport) -> String {
    [
        "event=copybot_activation_checklist_report".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("prod_config_path={}", report.prod_config_path),
        format!("non_prod_config_path={}", report.non_prod_config_path),
        format!("prod_config_env={}", report.prod_config_env),
        format!("non_prod_config_env={}", report.non_prod_config_env),
        format!("execution_enabled={}", report.execution_enabled),
        format!("planning_safe_only={}", report.planning_safe_only),
        format!("activation_authorized={}", report.activation_authorized),
        format!("discussion_ready_only={}", report.discussion_ready_only),
        format!(
            "prod_stage3_remains_hard_gate={}",
            report.prod_stage3_remains_hard_gate
        ),
        format!(
            "non_prod_evidence_is_secondary={}",
            report.non_prod_evidence_is_secondary
        ),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("blockers={}", report.blockers.join(" | ")),
        format!(
            "prod_pre_activation_gate_verdict={}",
            report.prod_pre_activation_gate.verdict
        ),
        format!(
            "prod_stage3_verdict={}",
            report.prod_pre_activation_gate.stage3_verdict
        ),
        format!("launch_dossier_verdict={}", report.launch_dossier.verdict),
        format!(
            "tiny_live_guardrail_verdict={}",
            report.tiny_live_guardrails.verdict
        ),
        format!(
            "non_prod_readiness_verdict={}",
            report.non_prod_readiness.verdict
        ),
        format!(
            "non_prod_activation_recent_internal_consistency_count={}",
            report
                .non_prod_readiness
                .activation_recent_internal_consistency_count
        ),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prod_stage3_blocked_keeps_top_level_blocked() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("blocked_by_stage3", "Stage 3 still drifting", false, true),
            ready_launch_dossier(),
            bounded_guardrails(),
            green_non_prod_readiness(),
        );

        assert_eq!(
            report.verdict,
            ActivationChecklistVerdict::ActivationChecklistBlockedByProdStage3
        );
    }

    #[test]
    fn non_prod_blocked_blocks_after_green_prod_gate_and_dossier() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("pre_activation_gates_green", "green", true, false),
            ready_launch_dossier(),
            bounded_guardrails(),
            non_prod_readiness(
                "devnet_readiness_blocked_by_activation_drill_history",
                "blocked",
                false,
            ),
        );

        assert_eq!(
            report.verdict,
            ActivationChecklistVerdict::ActivationChecklistBlockedByNonProdReadiness
        );
    }

    #[test]
    fn all_green_yields_discussion_ready_but_not_authorized() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("pre_activation_gates_green", "green", true, false),
            ready_launch_dossier(),
            bounded_guardrails(),
            green_non_prod_readiness(),
        );

        assert_eq!(
            report.verdict,
            ActivationChecklistVerdict::ActivationChecklistDiscussionReadyButNotAuthorized
        );
        assert!(report.discussion_ready_only);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn stale_non_prod_evidence_cannot_yield_discussion_ready() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("pre_activation_gates_green", "green", true, false),
            ready_launch_dossier(),
            bounded_guardrails(),
            non_prod_readiness("devnet_readiness_stale_history", "stale drills", false),
        );

        assert_eq!(
            report.verdict,
            ActivationChecklistVerdict::ActivationChecklistBlockedByNonProdReadiness
        );
    }

    #[test]
    fn human_output_is_explicitly_not_authorized() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("pre_activation_gates_green", "green", true, false),
            ready_launch_dossier(),
            bounded_guardrails(),
            green_non_prod_readiness(),
        );

        let human = render_human(&report);
        assert!(human.contains("activation_authorized=false"));
        assert!(human.contains("discussion_ready_only=true"));
    }

    #[test]
    fn execution_remains_disabled_in_report() {
        let report = compose_report(
            &test_config(),
            "prod-live",
            false,
            prod_gate_summary("pre_activation_gates_green", "green", true, false),
            ready_launch_dossier(),
            bounded_guardrails(),
            green_non_prod_readiness(),
        );

        assert!(!report.execution_enabled);
        assert!(report
            .blockers
            .iter()
            .any(|entry| entry.contains("execution.enabled=false")));
    }

    fn test_config() -> Config {
        Config {
            config_path: PathBuf::from("/etc/solana-copy-bot/live.server.toml"),
            non_prod_config_path: PathBuf::from("/etc/solana-copy-bot/devnet.server.toml"),
            json: false,
            now: ts("2026-03-26T12:00:00Z"),
            stage3_limit: DEFAULT_HISTORY_CAPTURE_LIMIT,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit: tiny_live_activation_plan::DEFAULT_REHEARSAL_HISTORY_LIMIT,
            rehearsal_recent_horizon_seconds:
                tiny_live_activation_plan::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
            min_recent_acceptable_rehearsals:
                tiny_live_activation_plan::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
            non_prod_limit: devnet_readiness_report::DEFAULT_HISTORY_LIMIT,
            non_prod_dress_recent_horizon_seconds:
                devnet_readiness_report::DEFAULT_DRESS_RECENT_HORIZON_SECONDS,
            non_prod_activation_recent_horizon_seconds:
                devnet_readiness_report::DEFAULT_ACTIVATION_RECENT_HORIZON_SECONDS,
            non_prod_min_recent_green_dress:
                devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_DRESS,
            non_prod_min_recent_green_activation:
                devnet_readiness_report::DEFAULT_MIN_RECENT_GREEN_ACTIVATION,
        }
    }

    fn prod_gate_summary(
        verdict: &str,
        reason: &str,
        planning_green: bool,
        blocked_by_stage3: bool,
    ) -> ProdPreActivationGateSummary {
        ProdPreActivationGateSummary {
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            planning_green,
            blocked_by_stage3,
            stage3_verdict: if blocked_by_stage3 {
                "publication_drifting".to_string()
            } else {
                "validated_current".to_string()
            },
            stage3_reason: reason.to_string(),
            stage3_captures_within_recent_horizon: 3,
            stage3_latest_capture_age_seconds: Some(60),
            stage4_readiness_verdict: "ready_for_execution_dry_run".to_string(),
            stage4_rehearsal_history_verdict: "sufficient_recent_rehearsal_evidence".to_string(),
            tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
            blockers: if planning_green {
                Vec::new()
            } else {
                vec![reason.to_string()]
            },
        }
    }

    fn ready_launch_dossier() -> LaunchDossierSummary {
        LaunchDossierSummary {
            verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
            reason: "ready".to_string(),
            ready_when_stage_gate_allows: true,
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            activation_overlay_change_count: 8,
            rollback_overlay_change_count: 8,
            drift_finding_count: 0,
            blocker_count: 0,
            first_blocker: None,
        }
    }

    fn bounded_guardrails() -> GuardrailSummary {
        GuardrailSummary {
            verdict: "tiny_live_guardrails_bounded".to_string(),
            reason: "bounded".to_string(),
            bounded: true,
            enabled: true,
            blocker_count: 0,
            first_blocker: None,
            rollback_trigger_count: 1,
            rollback_triggers: vec![tiny_live_guardrail_audit::RollbackTriggerSummary {
                trigger: "consecutive_hard_failures".to_string(),
                threshold_kind: "count".to_string(),
                threshold_rate_pct: None,
                threshold_seconds: None,
                threshold_sol: None,
                threshold_count: Some(3),
                evaluation_window_seconds: Some(900),
                action: "rollback_to_safe_mode".to_string(),
            }],
        }
    }

    fn green_non_prod_readiness() -> NonProdReadinessSummary {
        NonProdReadinessSummary {
            verdict: "devnet_readiness_green".to_string(),
            reason: "green".to_string(),
            green: true,
            prod_profile_refused: false,
            config_env: "paper-devnet".to_string(),
            blockers: Vec::new(),
            warnings: vec!["non-prod evidence only".to_string()],
            dress_latest_record_age_seconds: Some(120),
            dress_recent_green_count: 2,
            activation_latest_record_age_seconds: Some(180),
            activation_recent_green_count: 2,
            activation_recent_rollback_success_count: 2,
            activation_recent_internal_consistency_count: 2,
            stale_evidence_excluded: false,
        }
    }

    fn non_prod_readiness(verdict: &str, reason: &str, green: bool) -> NonProdReadinessSummary {
        NonProdReadinessSummary {
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            green,
            prod_profile_refused: false,
            config_env: "paper-devnet".to_string(),
            blockers: if green {
                Vec::new()
            } else {
                vec![reason.to_string()]
            },
            warnings: vec!["non-prod evidence only".to_string()],
            dress_latest_record_age_seconds: Some(120),
            dress_recent_green_count: 1,
            activation_latest_record_age_seconds: Some(180),
            activation_recent_green_count: usize::from(green),
            activation_recent_rollback_success_count: usize::from(green),
            activation_recent_internal_consistency_count: usize::from(green),
            stale_evidence_excluded: verdict == "devnet_readiness_stale_history",
        }
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }
}
