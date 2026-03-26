use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use copybot_storage::{ExecutionDevnetActivationDrillWrite, SqliteStore};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_devnet_dress_rehearsal.rs"]
mod devnet_dress_rehearsal;
#[allow(dead_code)]
#[path = "copybot_execution_dry_run_rehearsal.rs"]
mod execution_dry_run_rehearsal;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_plan.rs"]
mod tiny_live_activation_plan;
#[allow(dead_code)]
#[path = "copybot_tiny_live_guardrail_audit.rs"]
mod tiny_live_guardrail_audit;
#[allow(dead_code)]
#[path = "copybot_tiny_live_policy_audit.rs"]
mod tiny_live_policy_audit;

const USAGE: &str = "usage: copybot_devnet_activation_drill --config <path> [--route <route> --token <mint> --notional-sol <value>] [--history] [--limit <n>] [--json]";
const TARGET_ENVIRONMENT: &str = "devnet_activation_drill";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for devnet activation drill")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    route: Option<String>,
    token: Option<String>,
    notional_sol: Option<f64>,
    history: bool,
    limit: usize,
    json: bool,
}

#[derive(Debug, Clone, Serialize)]
struct DrillIntent {
    route: String,
    token: String,
    side: String,
    notional_sol: f64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DevnetActivationDrillVerdict {
    DevnetActivationDrillGreen,
    DevnetActivationDrillBlockedByLaunchDossier,
    DevnetActivationDrillBlockedByNonProdContract,
    DevnetActivationDrillBlockedByGuardrails,
    DevnetRollbackDrillFailed,
    DevnetActivationDrillRefusedForProdProfile,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RollbackDrillVerdict {
    RollbackDrillGreen,
    RollbackDrillFailed,
}

#[derive(Debug, Clone, Serialize)]
struct ActivationDrillSummary {
    verdict: String,
    reason: String,
    launch_dossier_viable_for_non_prod_drill: bool,
    policy_bounded_after_activation_overlay: bool,
    guardrails_bounded_after_activation_overlay: bool,
    rehearsal_verdict: Option<String>,
    rehearsal_reason: Option<String>,
    rehearsal_would_be_admissible_for_later_tiny_live: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct RollbackDrillSummary {
    verdict: RollbackDrillVerdict,
    reason: String,
    restores_safe_mode_contract: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LaunchDossierSummary {
    verdict: String,
    reason: String,
    pre_activation_gate_verdict: String,
    pre_activation_gate_reason: String,
    pre_activation_gate_green: bool,
    pre_activation_gate_blocked_for_prod: bool,
    tiny_live_policy_verdict: String,
    tiny_live_policy_bounded: bool,
    tiny_live_guardrail_verdict: String,
    tiny_live_guardrails_bounded: bool,
    activation_overlay_complete: bool,
    rollback_plan_complete: bool,
    service_restart_contract_complete: bool,
    activation_overlay_change_count: usize,
    rollback_overlay_change_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetActivationDrillReport {
    generated_at: DateTime<Utc>,
    drill_id: Option<i64>,
    config_path: String,
    db_path: String,
    target_environment: String,
    config_env: String,
    prod_profile_refused: bool,
    production_unchanged: bool,
    planning_safe_for_production: bool,
    stage3_prod_gate_still_required: bool,
    execution_enabled_source: bool,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    intent: DrillIntent,
    launch_dossier: LaunchDossierSummary,
    activation_drill: ActivationDrillSummary,
    rollback_drill: RollbackDrillSummary,
    activated_config_policy: Option<tiny_live_policy_audit::TinyLivePolicyAuditReport>,
    activated_config_guardrails: Option<tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport>,
    activation_rehearsal: Option<devnet_dress_rehearsal::DevnetDressRehearsalReport>,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetActivationDrillHistoryEntry {
    drill_id: i64,
    drilled_at: DateTime<Utc>,
    target_environment: String,
    config_env: String,
    verdict: String,
    reason: String,
    route: String,
    token: String,
    side: String,
    notional_sol: f64,
    launch_dossier_verdict: String,
    pre_activation_gate_verdict: String,
    tiny_live_policy_verdict: String,
    tiny_live_guardrail_verdict: String,
    activation_drill_verdict: String,
    rollback_drill_verdict: String,
    activated_config_policy_bounded: bool,
    activated_config_guardrails_bounded: bool,
    rollback_restores_safe_mode: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetActivationDrillHistoryReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    target_environment: String,
    records_loaded: usize,
    latest_drilled_at: Option<DateTime<Utc>>,
    verdict_counts: BTreeMap<String, usize>,
    drills: Vec<DevnetActivationDrillHistoryEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct DevnetActivationDrillHistoryRefusalReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    db_path: String,
    target_environment: String,
    config_env: String,
    history_mode: bool,
    prod_profile_refused: bool,
    production_unchanged: bool,
    planning_safe_for_production: bool,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
}

#[derive(Debug, Clone, Copy)]
enum OverlayTarget {
    Activation,
    Rollback,
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
    let mut route: Option<String> = None;
    let mut token: Option<String> = None;
    let mut notional_sol: Option<f64> = None;
    let mut history = false;
    let mut limit = 10usize;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--route" => route = Some(parse_string_arg("--route", args.next())?),
            "--token" => token = Some(parse_string_arg("--token", args.next())?),
            "--notional-sol" => notional_sol = Some(parse_f64_arg("--notional-sol", args.next())?),
            "--history" => history = true,
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if !history {
        let notional = notional_sol.ok_or_else(|| anyhow!("missing required --notional-sol"))?;
        if !notional.is_finite() || notional <= 0.0 {
            bail!("--notional-sol must be finite and > 0");
        }
        if route.is_none() {
            bail!("missing required --route");
        }
        if token.is_none() {
            bail!("missing required --token");
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        route,
        token,
        notional_sol,
        history,
        limit,
        json,
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid usize value for {flag}: {raw}"))
}

fn parse_f64_arg(flag: &str, value: Option<String>) -> Result<f64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<f64>()
        .with_context(|| format!("invalid f64 value for {flag}: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = execution_dry_run_rehearsal::resolve_db_path(
        &config.config_path,
        &loaded_config.sqlite.path,
    );

    if config.history {
        return render_history_mode_output(
            &config.config_path,
            &db_path,
            &loaded_config,
            config.limit,
            config.json,
        );
    }

    let route = config.route.as_deref().expect("route validated");
    let token = config.token.as_deref().expect("token validated");
    let notional_sol = config.notional_sol.expect("notional validated");
    let mut report = evaluate_devnet_activation_drill(
        &config.config_path,
        &db_path,
        &loaded_config,
        route,
        token,
        notional_sol,
    )
    .await?;
    if !report.prod_profile_refused {
        let store = SqliteStore::open(&db_path).with_context(|| {
            format!(
                "failed opening devnet activation drill history store {}",
                db_path.display()
            )
        })?;
        let persisted = store
            .append_execution_devnet_activation_drill(&persisted_row_for_report(&report)?)
            .context("failed persisting devnet activation drill history")?;
        report.drill_id = Some(persisted.drill_id);
    }
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing devnet activation drill json")
    } else {
        Ok(render_human(&report))
    }
}

async fn evaluate_devnet_activation_drill(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    route: &str,
    token: &str,
    notional_sol: f64,
) -> Result<DevnetActivationDrillReport> {
    if devnet_dress_rehearsal::is_production_like_env(loaded_config.system.env.as_str()) {
        return Ok(prod_refusal_report_without_plan(
            config_path,
            db_path,
            loaded_config,
            DrillIntent {
                route: route.trim().to_ascii_lowercase(),
                token: token.trim().to_string(),
                side: "buy".to_string(),
                notional_sol,
            },
        ));
    }
    let plan_config = tiny_live_activation_plan::Config {
        config_path: config_path.to_path_buf(),
        json: true,
        output_path: None,
        now: Utc::now(),
        stage3_limit: copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT,
        stage3_recent_horizon_seconds: None,
        rehearsal_limit: tiny_live_activation_plan::DEFAULT_REHEARSAL_HISTORY_LIMIT,
        rehearsal_recent_horizon_seconds:
            tiny_live_activation_plan::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
        min_recent_acceptable_rehearsals:
            tiny_live_activation_plan::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
    };
    let activation_plan =
        tiny_live_activation_plan::evaluate_tiny_live_activation_plan(&plan_config, loaded_config)
            .await?;
    evaluate_devnet_activation_drill_with_plan(
        config_path,
        db_path,
        loaded_config,
        route,
        token,
        notional_sol,
        activation_plan,
    )
    .await
}

async fn evaluate_devnet_activation_drill_with_plan(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    route: &str,
    token: &str,
    notional_sol: f64,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> Result<DevnetActivationDrillReport> {
    let intent = DrillIntent {
        route: route.trim().to_ascii_lowercase(),
        token: token.trim().to_string(),
        side: "buy".to_string(),
        notional_sol,
    };

    if devnet_dress_rehearsal::is_production_like_env(loaded_config.system.env.as_str()) {
        return Ok(prod_refusal_report(
            config_path,
            db_path,
            loaded_config,
            activation_plan,
            intent,
        ));
    }

    let launch_summary = build_launch_dossier_summary(&activation_plan);
    let mut warnings = vec![
        "Stage 3 discovery evidence remains the production gate; non-production activation drill does not authorize prod activation".to_string(),
        format!(
            "system.env={} is treated as non-production for this command; production activation state is unchanged",
            loaded_config.system.env
        ),
    ];
    let mut blockers = Vec::new();

    if let Some((verdict, reason, mut new_blockers)) = launch_dossier_blocker(
        &activation_plan,
        launch_summary.pre_activation_gate_blocked_for_prod,
    ) {
        blockers.append(&mut new_blockers);
        blockers.sort();
        blockers.dedup();
        warnings.sort();
        warnings.dedup();
        return Ok(report_without_drill(
            config_path,
            db_path,
            loaded_config,
            intent,
            activation_plan,
            launch_summary,
            verdict,
            reason,
            blockers,
            warnings,
        ));
    }

    if launch_summary.pre_activation_gate_blocked_for_prod {
        warnings.push(
            "pre-activation gate is still blocked in this source profile; the non-prod drill continues only to rehearse the bounded launch dossier, not to override the prod gate"
                .to_string(),
        );
    }

    let activated_config = apply_overlay_from_plan(
        loaded_config.clone(),
        &activation_plan.activation_overlay_changes,
        OverlayTarget::Activation,
    )
    .context("failed applying activation overlay to derived non-prod config")?;

    let activated_policy =
        tiny_live_policy_audit::evaluate_tiny_live_policy(config_path, &activated_config)?;
    let activated_guardrails =
        tiny_live_guardrail_audit::evaluate_tiny_live_guardrails(config_path, &activated_config)?;

    if !activated_guardrails.current_config_bounded_for_later_tiny_live_monitoring {
        blockers.push(format!(
            "activated_config_guardrails: {}",
            activated_guardrails.reason
        ));
        blockers.sort();
        blockers.dedup();
        warnings.extend(activated_guardrails.warnings.iter().cloned());
        warnings.sort();
        warnings.dedup();
        return Ok(report_with_static_outcome(
            config_path,
            db_path,
            loaded_config,
            intent,
            activation_plan,
            launch_summary,
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByGuardrails,
            format!(
                "derived activated config is not bounded by the accepted tiny-live guardrail envelope: {}",
                activated_guardrails.reason
            ),
            blockers,
            warnings,
            Some(activated_policy),
            Some(activated_guardrails),
        ));
    }

    if !activated_policy.current_config_bounded_for_later_tiny_live_discussion {
        blockers.push(format!(
            "activated_config_policy: {}",
            activated_policy.reason
        ));
        blockers.sort();
        blockers.dedup();
        warnings.extend(activated_policy.warnings.iter().cloned());
        warnings.sort();
        warnings.dedup();
        return Ok(report_with_static_outcome(
            config_path,
            db_path,
            loaded_config,
            intent,
            activation_plan,
            launch_summary,
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            format!(
                "derived activated config is not bounded by the accepted tiny-live policy envelope: {}",
                activated_policy.reason
            ),
            blockers,
            warnings,
            Some(activated_policy),
            Some(activated_guardrails),
        ));
    }

    let rehearsal = devnet_dress_rehearsal::evaluate_devnet_dress_rehearsal(
        config_path,
        db_path,
        &activated_config,
        intent.route.as_str(),
        intent.token.as_str(),
        intent.notional_sol,
    )
    .await?;

    let (verdict, reason) = classify_rehearsal_outcome(&rehearsal);
    if verdict != DevnetActivationDrillVerdict::DevnetActivationDrillGreen {
        blockers.extend(rehearsal.blockers.iter().cloned());
        warnings.extend(rehearsal.warnings.iter().cloned());
        blockers.sort();
        blockers.dedup();
        warnings.sort();
        warnings.dedup();
        return Ok(report_with_dynamic_outcome(
            config_path,
            db_path,
            loaded_config,
            intent,
            activation_plan,
            launch_summary,
            verdict,
            reason,
            blockers,
            warnings,
            activated_policy,
            activated_guardrails,
            rehearsal,
            RollbackDrillSummary {
                verdict: RollbackDrillVerdict::RollbackDrillGreen,
                reason: "rollback drill was not evaluated because activation drill did not pass"
                    .to_string(),
                restores_safe_mode_contract: false,
            },
        ));
    }

    let rolled_back_config = apply_overlay_from_plan(
        activated_config,
        &activation_plan.activation_overlay_changes,
        OverlayTarget::Rollback,
    )
    .context("failed applying rollback overlay to derived activated config")?;
    let rollback_restores_safe_mode =
        rollback_restores_source_contract(loaded_config, &rolled_back_config);
    let rollback_summary = if rollback_restores_safe_mode {
        RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillGreen,
            reason:
                "rollback overlay returned the derived activated config to the original safe-mode contract"
                    .to_string(),
            restores_safe_mode_contract: true,
        }
    } else {
        RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillFailed,
            reason:
                "rollback overlay did not restore the original safe-mode config contract exactly"
                    .to_string(),
            restores_safe_mode_contract: false,
        }
    };

    warnings.extend(activated_policy.warnings.iter().cloned());
    warnings.extend(activated_guardrails.warnings.iter().cloned());
    warnings.extend(rehearsal.warnings.iter().cloned());
    warnings.sort();
    warnings.dedup();

    let top_level_verdict = if rollback_summary.verdict == RollbackDrillVerdict::RollbackDrillGreen
    {
        DevnetActivationDrillVerdict::DevnetActivationDrillGreen
    } else {
        DevnetActivationDrillVerdict::DevnetRollbackDrillFailed
    };
    let top_level_reason = if top_level_verdict
        == DevnetActivationDrillVerdict::DevnetActivationDrillGreen
    {
        "bounded launch dossier remained internally consistent under non-production activation and rollback drill".to_string()
    } else {
        rollback_summary.reason.clone()
    };

    Ok(build_report(
        config_path,
        db_path,
        loaded_config,
        intent,
        activation_plan,
        launch_summary,
        top_level_verdict,
        top_level_reason,
        blockers,
        warnings,
        Some(activated_policy),
        Some(activated_guardrails),
        Some(rehearsal),
        rollback_summary,
    ))
}

fn build_launch_dossier_summary(
    activation_plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> LaunchDossierSummary {
    LaunchDossierSummary {
        verdict: serialize_enum(&activation_plan.verdict),
        reason: activation_plan.reason.clone(),
        pre_activation_gate_verdict: activation_plan.pre_activation_gate.verdict.clone(),
        pre_activation_gate_reason: activation_plan.pre_activation_gate.reason.clone(),
        pre_activation_gate_green: activation_plan.pre_activation_gate.planning_green,
        pre_activation_gate_blocked_for_prod: !activation_plan.pre_activation_gate.planning_green,
        tiny_live_policy_verdict: activation_plan.tiny_live_policy.verdict.clone(),
        tiny_live_policy_bounded: activation_plan.tiny_live_policy.bounded,
        tiny_live_guardrail_verdict: activation_plan.tiny_live_guardrails.verdict.clone(),
        tiny_live_guardrails_bounded: activation_plan.tiny_live_guardrails.bounded,
        activation_overlay_complete: activation_plan.activation_overlay_complete,
        rollback_plan_complete: activation_plan.rollback_plan_complete,
        service_restart_contract_complete: activation_plan.service_restart_contract_complete,
        activation_overlay_change_count: activation_plan.activation_overlay_change_count,
        rollback_overlay_change_count: activation_plan.rollback_overlay_change_count,
    }
}

fn launch_dossier_blocker(
    activation_plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
    pre_activation_gate_blocked_for_prod: bool,
) -> Option<(DevnetActivationDrillVerdict, String, Vec<String>)> {
    if !activation_plan.tiny_live_policy.bounded {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            format!(
                "accepted tiny-live launch dossier is blocked by the policy contract: {}",
                activation_plan.tiny_live_policy.reason
            ),
            vec![format!(
                "tiny_live_policy: {}",
                activation_plan.tiny_live_policy.reason
            )],
        ));
    }
    if !activation_plan.tiny_live_guardrails.bounded {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByGuardrails,
            format!(
                "accepted tiny-live launch dossier is blocked by the guardrail contract: {}",
                activation_plan.tiny_live_guardrails.reason
            ),
            vec![format!(
                "tiny_live_guardrails: {}",
                activation_plan.tiny_live_guardrails.reason
            )],
        ));
    }
    if !activation_plan.activation_overlay_complete {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            "accepted tiny-live launch dossier has an incomplete activation overlay".to_string(),
            activation_plan
                .blockers
                .iter()
                .filter(|blocker| blocker.contains("activation_overlay"))
                .cloned()
                .collect(),
        ));
    }
    if !activation_plan.rollback_plan_complete {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            "accepted tiny-live launch dossier has an incomplete rollback overlay".to_string(),
            vec!["rollback_plan: incomplete".to_string()],
        ));
    }
    if !activation_plan.service_restart_contract_complete {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            "accepted tiny-live launch dossier has an incomplete service restart contract"
                .to_string(),
            vec!["service_restart_contract: incomplete".to_string()],
        ));
    }
    if activation_plan.effective_tiny_live_contract.is_none() {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            "accepted tiny-live launch dossier has no effective bounded contract to rehearse"
                .to_string(),
            vec!["effective_tiny_live_contract: missing".to_string()],
        ));
    }
    if matches!(
        activation_plan.verdict,
        tiny_live_activation_plan::TinyLiveActivationPlanVerdict::BlockedByPolicyContract
            | tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationOverlayIncomplete
            | tiny_live_activation_plan::TinyLiveActivationPlanVerdict::RollbackPlanIncomplete
            | tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ServiceRestartContractIncomplete
    ) {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            activation_plan.reason.clone(),
            activation_plan.blockers.clone(),
        ));
    }
    if matches!(
        activation_plan.verdict,
        tiny_live_activation_plan::TinyLiveActivationPlanVerdict::BlockedByGuardrailContract
    ) {
        return Some((
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByGuardrails,
            activation_plan.reason.clone(),
            activation_plan.blockers.clone(),
        ));
    }
    if pre_activation_gate_blocked_for_prod {
        return None;
    }
    None
}

fn classify_rehearsal_outcome(
    rehearsal: &devnet_dress_rehearsal::DevnetDressRehearsalReport,
) -> (DevnetActivationDrillVerdict, String) {
    match rehearsal.verdict {
        devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalGreen => (
            DevnetActivationDrillVerdict::DevnetActivationDrillGreen,
            "derived activated config survived the accepted non-production dress rehearsal".to_string(),
        ),
        devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalGreenWithBusinessReject => (
            DevnetActivationDrillVerdict::DevnetActivationDrillGreen,
            "derived activated config survived the non-production dress rehearsal; adapter simulate returned a business reject, but the execution-side contract remained coherent".to_string(),
        ),
        devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByPolicyContract
        | devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalInputInvalid => (
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier,
            rehearsal.reason.clone(),
        ),
        devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByConnectivity
        | devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByAdapterContract
        | devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalBlockedByStaticContract => (
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByNonProdContract,
            rehearsal.reason.clone(),
        ),
        devnet_dress_rehearsal::DevnetDressRehearsalVerdict::DevnetRehearsalRefusedForProdProfile => (
            DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
            rehearsal.reason.clone(),
        ),
    }
}

fn report_without_drill(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: DrillIntent,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    launch_dossier: LaunchDossierSummary,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
) -> DevnetActivationDrillReport {
    build_report(
        config_path,
        db_path,
        loaded_config,
        intent,
        activation_plan,
        launch_dossier,
        verdict,
        reason,
        blockers,
        warnings,
        None,
        None,
        None,
        RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillFailed,
            reason: "rollback drill was not evaluated because activation drill did not run"
                .to_string(),
            restores_safe_mode_contract: false,
        },
    )
}

fn report_with_static_outcome(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: DrillIntent,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    launch_dossier: LaunchDossierSummary,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    activated_policy: Option<tiny_live_policy_audit::TinyLivePolicyAuditReport>,
    activated_guardrails: Option<tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport>,
) -> DevnetActivationDrillReport {
    build_report(
        config_path,
        db_path,
        loaded_config,
        intent,
        activation_plan,
        launch_dossier,
        verdict,
        reason,
        blockers,
        warnings,
        activated_policy,
        activated_guardrails,
        None,
        RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillFailed,
            reason: "rollback drill was not evaluated because activation drill did not pass"
                .to_string(),
            restores_safe_mode_contract: false,
        },
    )
}

fn report_with_dynamic_outcome(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: DrillIntent,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    launch_dossier: LaunchDossierSummary,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    activated_policy: tiny_live_policy_audit::TinyLivePolicyAuditReport,
    activated_guardrails: tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport,
    rehearsal: devnet_dress_rehearsal::DevnetDressRehearsalReport,
    rollback_drill: RollbackDrillSummary,
) -> DevnetActivationDrillReport {
    build_report(
        config_path,
        db_path,
        loaded_config,
        intent,
        activation_plan,
        launch_dossier,
        verdict,
        reason,
        blockers,
        warnings,
        Some(activated_policy),
        Some(activated_guardrails),
        Some(rehearsal),
        rollback_drill,
    )
}

fn build_report(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: DrillIntent,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    launch_dossier: LaunchDossierSummary,
    verdict: DevnetActivationDrillVerdict,
    reason: String,
    mut blockers: Vec<String>,
    mut warnings: Vec<String>,
    activated_policy: Option<tiny_live_policy_audit::TinyLivePolicyAuditReport>,
    activated_guardrails: Option<tiny_live_guardrail_audit::TinyLiveGuardrailAuditReport>,
    rehearsal: Option<devnet_dress_rehearsal::DevnetDressRehearsalReport>,
    rollback_drill: RollbackDrillSummary,
) -> DevnetActivationDrillReport {
    blockers.sort();
    blockers.dedup();
    warnings.sort();
    warnings.dedup();

    let activation_drill = ActivationDrillSummary {
        verdict: serialize_enum(&verdict),
        reason: reason.clone(),
        launch_dossier_viable_for_non_prod_drill: verdict
            != DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier
            && verdict != DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByGuardrails,
        policy_bounded_after_activation_overlay: activated_policy
            .as_ref()
            .map(|report| report.current_config_bounded_for_later_tiny_live_discussion)
            .unwrap_or(false),
        guardrails_bounded_after_activation_overlay: activated_guardrails
            .as_ref()
            .map(|report| report.current_config_bounded_for_later_tiny_live_monitoring)
            .unwrap_or(false),
        rehearsal_verdict: rehearsal
            .as_ref()
            .map(|report| serialize_enum(&report.verdict)),
        rehearsal_reason: rehearsal.as_ref().map(|report| report.reason.clone()),
        rehearsal_would_be_admissible_for_later_tiny_live: rehearsal
            .as_ref()
            .and_then(|report| report.dry_run.as_ref())
            .map(|summary| summary.would_be_admissible_for_later_tiny_live),
    };

    DevnetActivationDrillReport {
        generated_at: Utc::now(),
        drill_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: false,
        production_unchanged: true,
        planning_safe_for_production: true,
        stage3_prod_gate_still_required: true,
        execution_enabled_source: loaded_config.execution.enabled,
        verdict,
        reason,
        blockers,
        warnings,
        intent,
        launch_dossier,
        activation_drill,
        rollback_drill,
        activated_config_policy: activated_policy,
        activated_config_guardrails: activated_guardrails,
        activation_rehearsal: rehearsal,
        activation_plan,
    }
}

fn prod_refusal_report(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    intent: DrillIntent,
) -> DevnetActivationDrillReport {
    let reason = format!(
        "copybot_devnet_activation_drill refuses production-like system.env={}",
        loaded_config.system.env
    );
    DevnetActivationDrillReport {
        generated_at: Utc::now(),
        drill_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        stage3_prod_gate_still_required: true,
        execution_enabled_source: loaded_config.execution.enabled,
        verdict: DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
        reason: reason.clone(),
        blockers: vec![reason],
        warnings: vec![
            "production activation remains untouched; use a non-production config profile for devnet activation drill".to_string(),
        ],
        intent,
        launch_dossier: build_launch_dossier_summary(&activation_plan),
        activation_drill: ActivationDrillSummary {
            verdict: serialize_enum(
                &DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
            ),
            reason: "non-production contract refused before any activation overlay rehearsal".to_string(),
            launch_dossier_viable_for_non_prod_drill: false,
            policy_bounded_after_activation_overlay: false,
            guardrails_bounded_after_activation_overlay: false,
            rehearsal_verdict: None,
            rehearsal_reason: None,
            rehearsal_would_be_admissible_for_later_tiny_live: None,
        },
        rollback_drill: RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillFailed,
            reason: "rollback drill not evaluated after production-profile refusal".to_string(),
            restores_safe_mode_contract: false,
        },
        activated_config_policy: None,
        activated_config_guardrails: None,
        activation_rehearsal: None,
        activation_plan,
    }
}

fn prod_refusal_report_without_plan(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    intent: DrillIntent,
) -> DevnetActivationDrillReport {
    let reason = format!(
        "copybot_devnet_activation_drill refuses production-like system.env={}",
        loaded_config.system.env
    );
    DevnetActivationDrillReport {
        generated_at: Utc::now(),
        drill_id: None,
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        prod_profile_refused: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        stage3_prod_gate_still_required: true,
        execution_enabled_source: loaded_config.execution.enabled,
        verdict: DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
        reason: reason.clone(),
        blockers: vec![reason],
        warnings: vec![
            "production activation remains untouched; use a non-production config profile for devnet activation drill".to_string(),
        ],
        intent,
        launch_dossier: LaunchDossierSummary {
            verdict: "not_evaluated_due_to_prod_refusal".to_string(),
            reason: "launch dossier was not evaluated because the command refused a production-like config profile".to_string(),
            pre_activation_gate_verdict: "not_evaluated".to_string(),
            pre_activation_gate_reason: "not evaluated".to_string(),
            pre_activation_gate_green: false,
            pre_activation_gate_blocked_for_prod: true,
            tiny_live_policy_verdict: "not_evaluated".to_string(),
            tiny_live_policy_bounded: false,
            tiny_live_guardrail_verdict: "not_evaluated".to_string(),
            tiny_live_guardrails_bounded: false,
            activation_overlay_complete: false,
            rollback_plan_complete: false,
            service_restart_contract_complete: false,
            activation_overlay_change_count: 0,
            rollback_overlay_change_count: 0,
        },
        activation_drill: ActivationDrillSummary {
            verdict: serialize_enum(
                &DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
            ),
            reason: "non-production contract refused before any activation overlay rehearsal".to_string(),
            launch_dossier_viable_for_non_prod_drill: false,
            policy_bounded_after_activation_overlay: false,
            guardrails_bounded_after_activation_overlay: false,
            rehearsal_verdict: None,
            rehearsal_reason: None,
            rehearsal_would_be_admissible_for_later_tiny_live: None,
        },
        rollback_drill: RollbackDrillSummary {
            verdict: RollbackDrillVerdict::RollbackDrillFailed,
            reason: "rollback drill not evaluated after production-profile refusal".to_string(),
            restores_safe_mode_contract: false,
        },
        activated_config_policy: None,
        activated_config_guardrails: None,
        activation_rehearsal: None,
        activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport {
            generated_at: Utc::now(),
            config_path: config_path.display().to_string(),
            output_path: None,
            planning_safe_only: true,
            activation_permission_granted: false,
            execution_enabled_current: loaded_config.execution.enabled,
            current_execution_mode: loaded_config.execution.mode.clone(),
            verdict: tiny_live_activation_plan::TinyLiveActivationPlanVerdict::BlockedByPreActivationGate,
            reason: "not evaluated due to production-profile refusal".to_string(),
            blockers: vec!["launch dossier not evaluated on production-like profile".to_string()],
            drift_findings: Vec::new(),
            pre_activation_gate: tiny_live_activation_plan::ActivationPlanGateSummary {
                verdict: "not_evaluated".to_string(),
                reason: "not evaluated".to_string(),
                planning_green: false,
                blockers: Vec::new(),
                execution_enabled: loaded_config.execution.enabled,
                stage3_is_primary_gate: true,
            },
            tiny_live_policy: tiny_live_activation_plan::PolicyAuditSummary {
                verdict: "not_evaluated".to_string(),
                reason: "not evaluated".to_string(),
                bounded: false,
                tiny_live_policy_enabled: loaded_config.tiny_live_policy.enabled,
                blocker_count: 0,
                first_blocker: None,
                warnings_count: 0,
            },
            tiny_live_guardrails: tiny_live_activation_plan::GuardrailAuditSummary {
                verdict: "not_evaluated".to_string(),
                reason: "not evaluated".to_string(),
                bounded: false,
                tiny_live_guardrails_enabled: loaded_config.tiny_live_guardrails.enabled,
                first_blocker: None,
                blocker_count: 0,
                warnings_count: 0,
                rollback_triggers: Vec::new(),
            },
            activation_overlay_complete: false,
            rollback_plan_complete: false,
            service_restart_contract_complete: false,
            activation_overlay_change_count: 0,
            rollback_overlay_change_count: 0,
            activation_overlay_changes: Vec::new(),
            unchanged_fields: Vec::new(),
            effective_tiny_live_contract: None,
            service_restart_contract: tiny_live_activation_plan::ServiceRestartContract {
                complete: false,
                activation_restart_required: false,
                rollback_restart_required: false,
                activation_services: Vec::new(),
                rollback_services: Vec::new(),
                activation_steps: Vec::new(),
                rollback_steps: Vec::new(),
            },
        },
    }
}

fn render_history_mode_output(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
    limit: usize,
    json: bool,
) -> Result<String> {
    if devnet_dress_rehearsal::is_production_like_env(loaded_config.system.env.as_str()) {
        let report = history_prod_refusal_report(config_path, db_path, loaded_config);
        return if json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing devnet activation drill history refusal json")
        } else {
            Ok(render_history_refusal_human(&report))
        };
    }

    let store = SqliteStore::open(db_path).with_context(|| {
        format!(
            "failed opening devnet activation drill history store {}",
            db_path.display()
        )
    })?;
    let report = build_history_report(&store, config_path, db_path, limit)?;
    if json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing devnet activation drill history json")
    } else {
        Ok(render_history_human(&report))
    }
}

fn build_history_report(
    store: &SqliteStore,
    config_path: &Path,
    db_path: &Path,
    limit: usize,
) -> Result<DevnetActivationDrillHistoryReport> {
    let rows = store
        .list_execution_devnet_activation_drills(limit)
        .context("failed loading execution devnet activation drill history")?;
    let latest_drilled_at = rows.first().map(|row| row.drilled_at);
    let mut verdict_counts = BTreeMap::new();
    for row in &rows {
        *verdict_counts
            .entry(row.activation_drill_verdict.clone())
            .or_insert(0usize) += 1;
    }

    Ok(DevnetActivationDrillHistoryReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        records_loaded: rows.len(),
        latest_drilled_at,
        verdict_counts,
        drills: rows
            .into_iter()
            .map(|row| DevnetActivationDrillHistoryEntry {
                drill_id: row.drill_id,
                drilled_at: row.drilled_at,
                target_environment: row.target_environment,
                config_env: row.config_env,
                verdict: row.activation_drill_verdict.clone(),
                reason: row.activation_drill_reason.clone(),
                route: row.route,
                token: row.token,
                side: row.side,
                notional_sol: row.notional_sol,
                launch_dossier_verdict: row.launch_dossier_verdict,
                pre_activation_gate_verdict: row.pre_activation_gate_verdict,
                tiny_live_policy_verdict: row.tiny_live_policy_verdict,
                tiny_live_guardrail_verdict: row.tiny_live_guardrail_verdict,
                activation_drill_verdict: row.activation_drill_verdict,
                rollback_drill_verdict: row.rollback_drill_verdict,
                activated_config_policy_bounded: row.activated_config_policy_bounded,
                activated_config_guardrails_bounded: row.activated_config_guardrails_bounded,
                rollback_restores_safe_mode: row.rollback_restores_safe_mode,
                blockers: row.blockers,
                warnings: row.warnings,
            })
            .collect(),
    })
}

fn history_prod_refusal_report(
    config_path: &Path,
    db_path: &Path,
    loaded_config: &AppConfig,
) -> DevnetActivationDrillHistoryRefusalReport {
    DevnetActivationDrillHistoryRefusalReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        target_environment: TARGET_ENVIRONMENT.to_string(),
        config_env: loaded_config.system.env.clone(),
        history_mode: true,
        prod_profile_refused: true,
        production_unchanged: true,
        planning_safe_for_production: true,
        verdict: DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile,
        reason: format!(
            "copybot_devnet_activation_drill --history refuses production-like system.env={}",
            loaded_config.system.env
        ),
    }
}

fn apply_overlay_from_plan(
    mut config: AppConfig,
    changes: &[tiny_live_activation_plan::ActivationPlanFieldDelta],
    target: OverlayTarget,
) -> Result<AppConfig> {
    for change in changes {
        let value = match target {
            OverlayTarget::Activation => &change.activation_value,
            OverlayTarget::Rollback => &change.rollback_value,
        };
        apply_field_delta(&mut config, change.field.as_str(), value)
            .with_context(|| format!("failed applying field {}", change.field))?;
    }
    Ok(config)
}

fn apply_field_delta(config: &mut AppConfig, field: &str, value: &Value) -> Result<()> {
    match field {
        "execution.enabled" => config.execution.enabled = parse_bool_value(value, field)?,
        "execution.mode" => config.execution.mode = parse_string_value(value, field)?,
        "execution.default_route" => {
            config.execution.default_route = parse_string_value(value, field)?
        }
        "execution.submit_allowed_routes" => {
            config.execution.submit_allowed_routes = parse_vec_string_value(value, field)?
        }
        "execution.submit_route_order" => {
            config.execution.submit_route_order = parse_vec_string_value(value, field)?
        }
        "execution.submit_adapter_require_policy_echo" => {
            config.execution.submit_adapter_require_policy_echo = parse_bool_value(value, field)?
        }
        "shadow.copy_notional_sol" => {
            config.shadow.copy_notional_sol = parse_f64_value(value, field)?
        }
        "risk.max_position_sol" => config.risk.max_position_sol = parse_f64_value(value, field)?,
        "execution.batch_size" => config.execution.batch_size = parse_u32_value(value, field)?,
        "risk.max_concurrent_positions" => {
            config.risk.max_concurrent_positions = parse_u32_value(value, field)?
        }
        "risk.daily_loss_limit_pct" => {
            config.risk.daily_loss_limit_pct = parse_f64_value(value, field)?
        }
        "execution.pretrade_max_fee_overhead_bps" => {
            config.execution.pretrade_max_fee_overhead_bps = parse_u32_value(value, field)?
        }
        "execution.pretrade_max_priority_fee_lamports" => {
            config.execution.pretrade_max_priority_fee_lamports = parse_u64_value(value, field)?
        }
        "execution.submit_route_max_slippage_bps" => {
            config.execution.submit_route_max_slippage_bps =
                serde_json::from_value(value.clone())
                    .with_context(|| format!("invalid route->f64 map for {field}"))?
        }
        "execution.submit_route_tip_lamports" => {
            config.execution.submit_route_tip_lamports = serde_json::from_value(value.clone())
                .with_context(|| format!("invalid route->u64 map for {field}"))?
        }
        "execution.submit_route_compute_unit_price_micro_lamports" => {
            config
                .execution
                .submit_route_compute_unit_price_micro_lamports =
                serde_json::from_value(value.clone())
                    .with_context(|| format!("invalid route->u64 map for {field}"))?
        }
        "execution.submit_route_compute_unit_limit" => {
            config.execution.submit_route_compute_unit_limit = serde_json::from_value(value.clone())
                .with_context(|| format!("invalid route->u32 map for {field}"))?
        }
        other => bail!("unsupported activation-plan overlay field {other}"),
    }
    Ok(())
}

fn parse_string_value(value: &Value, field: &str) -> Result<String> {
    value
        .as_str()
        .map(|raw| raw.to_string())
        .ok_or_else(|| anyhow!("{field} must be a string"))
}

fn parse_bool_value(value: &Value, field: &str) -> Result<bool> {
    value
        .as_bool()
        .ok_or_else(|| anyhow!("{field} must be a bool"))
}

fn parse_f64_value(value: &Value, field: &str) -> Result<f64> {
    value
        .as_f64()
        .ok_or_else(|| anyhow!("{field} must be an f64"))
}

fn parse_u32_value(value: &Value, field: &str) -> Result<u32> {
    let raw = value
        .as_u64()
        .ok_or_else(|| anyhow!("{field} must be a u32-compatible integer"))?;
    u32::try_from(raw).with_context(|| format!("{field} exceeds u32"))
}

fn parse_u64_value(value: &Value, field: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow!("{field} must be a u64"))
}

fn parse_vec_string_value(value: &Value, field: &str) -> Result<Vec<String>> {
    serde_json::from_value(value.clone()).with_context(|| format!("{field} must be a string array"))
}

fn rollback_restores_source_contract(source: &AppConfig, rolled_back: &AppConfig) -> bool {
    source.execution.enabled == rolled_back.execution.enabled
        && source.execution.mode == rolled_back.execution.mode
        && source.execution.default_route == rolled_back.execution.default_route
        && source.execution.submit_allowed_routes == rolled_back.execution.submit_allowed_routes
        && source.execution.submit_route_order == rolled_back.execution.submit_route_order
        && source.execution.submit_adapter_require_policy_echo
            == rolled_back.execution.submit_adapter_require_policy_echo
        && source.shadow.copy_notional_sol == rolled_back.shadow.copy_notional_sol
        && source.risk.max_position_sol == rolled_back.risk.max_position_sol
        && source.execution.batch_size == rolled_back.execution.batch_size
        && source.risk.max_concurrent_positions == rolled_back.risk.max_concurrent_positions
        && source.risk.daily_loss_limit_pct == rolled_back.risk.daily_loss_limit_pct
        && source.execution.pretrade_max_fee_overhead_bps
            == rolled_back.execution.pretrade_max_fee_overhead_bps
        && source.execution.pretrade_max_priority_fee_lamports
            == rolled_back.execution.pretrade_max_priority_fee_lamports
        && source.execution.submit_route_max_slippage_bps
            == rolled_back.execution.submit_route_max_slippage_bps
        && source.execution.submit_route_tip_lamports
            == rolled_back.execution.submit_route_tip_lamports
        && source
            .execution
            .submit_route_compute_unit_price_micro_lamports
            == rolled_back
                .execution
                .submit_route_compute_unit_price_micro_lamports
        && source.execution.submit_route_compute_unit_limit
            == rolled_back.execution.submit_route_compute_unit_limit
}

fn persisted_row_for_report(
    report: &DevnetActivationDrillReport,
) -> Result<ExecutionDevnetActivationDrillWrite> {
    Ok(ExecutionDevnetActivationDrillWrite {
        drilled_at: report.generated_at,
        target_environment: report.target_environment.clone(),
        config_env: report.config_env.clone(),
        source_config_path: report.config_path.clone(),
        execution_enabled_source: report.execution_enabled_source,
        route: report.intent.route.clone(),
        token: report.intent.token.clone(),
        side: report.intent.side.clone(),
        notional_sol: report.intent.notional_sol,
        launch_dossier_verdict: report.launch_dossier.verdict.clone(),
        launch_dossier_reason: report.launch_dossier.reason.clone(),
        pre_activation_gate_verdict: report.launch_dossier.pre_activation_gate_verdict.clone(),
        pre_activation_gate_reason: report.launch_dossier.pre_activation_gate_reason.clone(),
        tiny_live_policy_verdict: report.launch_dossier.tiny_live_policy_verdict.clone(),
        tiny_live_guardrail_verdict: report.launch_dossier.tiny_live_guardrail_verdict.clone(),
        tiny_live_policy_bounded: report.launch_dossier.tiny_live_policy_bounded,
        tiny_live_guardrails_bounded: report.launch_dossier.tiny_live_guardrails_bounded,
        activation_overlay_change_count: report.launch_dossier.activation_overlay_change_count,
        rollback_overlay_change_count: report.launch_dossier.rollback_overlay_change_count,
        activation_drill_verdict: serialize_enum(&report.verdict),
        activation_drill_reason: report.reason.clone(),
        activation_rehearsal_verdict: report
            .activation_rehearsal
            .as_ref()
            .map(|rehearsal| serialize_enum(&rehearsal.verdict)),
        activation_rehearsal_reason: report
            .activation_rehearsal
            .as_ref()
            .map(|rehearsal| rehearsal.reason.clone()),
        rollback_drill_verdict: serialize_enum(&report.rollback_drill.verdict),
        rollback_drill_reason: report.rollback_drill.reason.clone(),
        activated_config_policy_bounded: report
            .activated_config_policy
            .as_ref()
            .map(|policy| policy.current_config_bounded_for_later_tiny_live_discussion)
            .unwrap_or(false),
        activated_config_guardrails_bounded: report
            .activated_config_guardrails
            .as_ref()
            .map(|guardrails| guardrails.current_config_bounded_for_later_tiny_live_monitoring)
            .unwrap_or(false),
        rollback_restores_safe_mode: report.rollback_drill.restores_safe_mode_contract,
        blockers: report.blockers.clone(),
        warnings: report.warnings.clone(),
        drill_json: serde_json::to_string(report)
            .context("failed serializing devnet activation drill report json")?,
    })
}

fn render_human(report: &DevnetActivationDrillReport) -> String {
    [
        "event=copybot_devnet_activation_drill".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!(
            "drill_id={}",
            report
                .drill_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "planning_safe_for_production={}",
            report.planning_safe_for_production
        ),
        format!(
            "stage3_prod_gate_still_required={}",
            report.stage3_prod_gate_still_required
        ),
        format!(
            "execution_enabled_source={}",
            report.execution_enabled_source
        ),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("launch_dossier_verdict={}", report.launch_dossier.verdict),
        format!(
            "pre_activation_gate_verdict={}",
            report.launch_dossier.pre_activation_gate_verdict
        ),
        format!(
            "pre_activation_gate_blocked_for_prod={}",
            report.launch_dossier.pre_activation_gate_blocked_for_prod
        ),
        format!(
            "tiny_live_policy_verdict={}",
            report.launch_dossier.tiny_live_policy_verdict
        ),
        format!(
            "tiny_live_guardrail_verdict={}",
            report.launch_dossier.tiny_live_guardrail_verdict
        ),
        format!(
            "activation_overlay_change_count={}",
            report.launch_dossier.activation_overlay_change_count
        ),
        format!(
            "rollback_overlay_change_count={}",
            report.launch_dossier.rollback_overlay_change_count
        ),
        format!(
            "activated_config_policy_bounded={}",
            report
                .activation_drill
                .policy_bounded_after_activation_overlay
        ),
        format!(
            "activated_config_guardrails_bounded={}",
            report
                .activation_drill
                .guardrails_bounded_after_activation_overlay
        ),
        format!(
            "activation_rehearsal_verdict={}",
            report
                .activation_drill
                .rehearsal_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "rollback_drill_verdict={}",
            serialize_enum(&report.rollback_drill.verdict)
        ),
        format!(
            "rollback_restores_safe_mode_contract={}",
            report.rollback_drill.restores_safe_mode_contract
        ),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn render_history_human(report: &DevnetActivationDrillHistoryReport) -> String {
    [
        "event=copybot_devnet_activation_drill_history".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
        format!("target_environment={}", report.target_environment),
        format!("records_loaded={}", report.records_loaded),
        format!(
            "latest_drilled_at={}",
            report
                .latest_drilled_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "verdict_counts={}",
            report
                .verdict_counts
                .iter()
                .map(|(verdict, count)| format!("{verdict}:{count}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    ]
    .join("\n")
}

fn render_history_refusal_human(report: &DevnetActivationDrillHistoryRefusalReport) -> String {
    [
        "event=copybot_devnet_activation_drill_history".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("history_mode={}", report.history_mode),
        format!("target_environment={}", report.target_environment),
        format!("config_env={}", report.config_env),
        format!("prod_profile_refused={}", report.prod_profile_refused),
        format!("production_unchanged={}", report.production_unchanged),
        format!(
            "planning_safe_for_production={}",
            report.planning_safe_for_production
        ),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("config_path={}", report.config_path),
        format!("db_path={}", report.db_path),
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
    use serde_json::Value;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    const WSOL: &str = "So11111111111111111111111111111111111111112";

    #[test]
    fn prod_like_config_is_refused() {
        let temp = temp_dir("devnet_activation_drill_prod_refused");
        let config_path = temp.join("prod.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.system.env = "prod-live".to_string();
        config.sqlite.path = temp.join("runtime.db").display().to_string();

        let report = run_report_with_plan(
            &config_path,
            &config,
            "jito",
            WSOL,
            0.01,
            ready_activation_plan_report(),
        );
        assert_eq!(
            report.verdict,
            DevnetActivationDrillVerdict::DevnetActivationDrillRefusedForProdProfile
        );
        assert!(report.prod_profile_refused);
    }

    #[test]
    fn non_prod_config_can_run_drill() {
        let temp = temp_dir("devnet_activation_drill_green");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({"jsonrpc":"2.0","id":1,"result":123456u64})),
                MockResponse::json(
                    json!({"jsonrpc":"2.0","id":1,"result":{"context":{"slot":123456u64},"value":{"blockhash":"abc123","lastValidBlockHeight":999u64}}}),
                ),
                MockResponse::json(
                    json!({"jsonrpc":"2.0","id":1,"result":{"context":{"slot":123456u64},"value":42u64}}),
                ),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "status":"ok","ok":true,"accepted":true,"route":"jito","contract_version":"v1","detail":"simulated"
                })),
                MockResponse::json(json!({
                    "status":"ok","ok":true,"accepted":true,"route":"jito","contract_version":"v1","detail":"simulated"
                })),
            ],
            Some("authorization"),
        );

        let mut config = base_devnet_config();
        let db_path = temp.join("runtime.db");
        config.sqlite.path = db_path.display().to_string();
        config.execution.rpc_devnet_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url =
            "https://prod.adapter.example.com/submit".to_string();
        config.execution.submit_adapter_devnet_http_url = adapter_server.url("/submit");

        let report = run_report_with_plan(
            &config_path,
            &config,
            "jito",
            WSOL,
            0.01,
            ready_activation_plan_report(),
        );
        assert_eq!(
            report.verdict,
            DevnetActivationDrillVerdict::DevnetActivationDrillGreen,
            "{}",
            serde_json::to_string_pretty(&report).expect("json")
        );
        assert!(report.rollback_drill.restores_safe_mode_contract);
        assert_eq!(
            report
                .activation_rehearsal
                .as_ref()
                .expect("rehearsal")
                .target_environment,
            "devnet"
        );
    }

    #[test]
    fn blocked_launch_dossier_blocks_activation_drill() {
        let temp = temp_dir("devnet_activation_drill_launch_blocked");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();

        let mut plan = ready_activation_plan_report();
        plan.activation_overlay_complete = false;
        plan.verdict =
            tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationOverlayIncomplete;
        plan.reason = "activation overlay incomplete".to_string();
        plan.blockers = vec!["activation_overlay: missing route cap".to_string()];

        let report = run_report_with_plan(&config_path, &config, "jito", WSOL, 0.01, plan);
        assert_eq!(
            report.verdict,
            DevnetActivationDrillVerdict::DevnetActivationDrillBlockedByLaunchDossier
        );
    }

    #[test]
    fn bounded_overlay_and_guardrails_yield_green_activation_drill() {
        let temp = temp_dir("devnet_activation_drill_guardrails_green");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let rpc_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({"jsonrpc":"2.0","id":1,"result":77u64})),
                MockResponse::json(
                    json!({"jsonrpc":"2.0","id":1,"result":{"context":{"slot":77u64},"value":{"blockhash":"abc","lastValidBlockHeight":80u64}}}),
                ),
                MockResponse::json(
                    json!({"jsonrpc":"2.0","id":1,"result":{"context":{"slot":77u64},"value":42u64}}),
                ),
            ],
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            vec![
                MockResponse::json(json!({
                    "status":"ok","ok":true,"accepted":true,"route":"jito","contract_version":"v1","detail":"simulated"
                })),
                MockResponse::json(json!({
                    "status":"ok","ok":true,"accepted":true,"route":"jito","contract_version":"v1","detail":"simulated"
                })),
            ],
            Some("authorization"),
        );

        let mut config = base_devnet_config();
        config.sqlite.path = temp.join("runtime.db").display().to_string();
        config.execution.rpc_devnet_http_url = rpc_server.url("");
        config.execution.submit_adapter_http_url =
            "https://prod.adapter.example.com/submit".to_string();
        config.execution.submit_adapter_devnet_http_url = adapter_server.url("/submit");

        let report = run_report_with_plan(
            &config_path,
            &config,
            "jito",
            WSOL,
            0.01,
            ready_activation_plan_report(),
        );
        assert_eq!(
            report.verdict,
            DevnetActivationDrillVerdict::DevnetActivationDrillGreen
        );
        assert!(
            report
                .activated_config_guardrails
                .as_ref()
                .expect("guardrails")
                .current_config_bounded_for_later_tiny_live_monitoring
        );
    }

    #[test]
    fn rollback_overlay_returns_safe_mode_contract() {
        let mut activated = base_devnet_config();
        activated.execution.enabled = true;
        activated.execution.default_route = "rpc".to_string();
        activated.execution.submit_allowed_routes = vec!["rpc".to_string()];
        activated.execution.submit_route_order = vec!["rpc".to_string()];
        activated.execution.submit_adapter_require_policy_echo = false;
        activated.shadow.copy_notional_sol = 0.01;
        activated.risk.max_position_sol = 0.01;
        activated.execution.batch_size = 4;
        activated.risk.max_concurrent_positions = 4;
        activated.risk.daily_loss_limit_pct = 2.0;
        activated.execution.pretrade_max_fee_overhead_bps = 2000;
        activated.execution.pretrade_max_priority_fee_lamports = 4_000;
        activated.execution.submit_route_max_slippage_bps =
            [("rpc".to_string(), 70.0)].into_iter().collect();
        activated.execution.submit_route_tip_lamports =
            [("rpc".to_string(), 50_000u64)].into_iter().collect();
        activated
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("rpc".to_string(), 8_000u64)].into_iter().collect();
        activated.execution.submit_route_compute_unit_limit =
            [("rpc".to_string(), 450_000u32)].into_iter().collect();

        assert!(rollback_restores_source_contract(
            &base_devnet_config(),
            &base_devnet_config()
        ));
        assert!(!rollback_restores_source_contract(
            &base_devnet_config(),
            &activated
        ));
    }

    #[test]
    fn history_is_persisted_with_environment_label() {
        let temp = temp_dir("devnet_activation_drill_history");
        let db_path = temp.join("runtime.db");
        let store = SqliteStore::open(&db_path).expect("open store");
        let row = store
            .append_execution_devnet_activation_drill(&ExecutionDevnetActivationDrillWrite {
                drilled_at: ts("2026-03-26T12:00:00Z"),
                target_environment: TARGET_ENVIRONMENT.to_string(),
                config_env: "paper-devnet".to_string(),
                source_config_path: "/tmp/devnet.server.toml".to_string(),
                execution_enabled_source: false,
                route: "jito".to_string(),
                token: WSOL.to_string(),
                side: "buy".to_string(),
                notional_sol: 0.01,
                launch_dossier_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
                launch_dossier_reason: "ready".to_string(),
                pre_activation_gate_verdict: "blocked_by_stage3".to_string(),
                pre_activation_gate_reason: "prod gate still blocked".to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                tiny_live_guardrail_verdict: "tiny_live_guardrails_bounded".to_string(),
                tiny_live_policy_bounded: true,
                tiny_live_guardrails_bounded: true,
                activation_overlay_change_count: 5,
                rollback_overlay_change_count: 5,
                activation_drill_verdict: "devnet_activation_drill_green".to_string(),
                activation_drill_reason: "green".to_string(),
                activation_rehearsal_verdict: Some("devnet_rehearsal_green".to_string()),
                activation_rehearsal_reason: Some("green".to_string()),
                rollback_drill_verdict: "rollback_drill_green".to_string(),
                rollback_drill_reason: "rollback okay".to_string(),
                activated_config_policy_bounded: true,
                activated_config_guardrails_bounded: true,
                rollback_restores_safe_mode: true,
                blockers: Vec::new(),
                warnings: vec!["prod unchanged".to_string()],
                drill_json: "{}".to_string(),
            })
            .expect("append row");
        assert_eq!(row.target_environment, TARGET_ENVIRONMENT);

        let report =
            build_history_report(&store, Path::new("/tmp/devnet.server.toml"), &db_path, 5)
                .expect("history");
        assert_eq!(report.records_loaded, 1);
        assert_eq!(report.drills[0].target_environment, TARGET_ENVIRONMENT);
    }

    #[test]
    fn execution_flags_on_source_config_are_unchanged() {
        let temp = temp_dir("devnet_activation_drill_source_flags");
        let config_path = temp.join("devnet.server.toml");
        write_placeholder_config(&config_path);

        let config = base_devnet_config();
        let before = config.execution.enabled;
        let report = run_report_with_plan(
            &config_path,
            &config,
            "jito",
            WSOL,
            0.01,
            ready_activation_plan_report(),
        );
        assert_eq!(config.execution.enabled, before);
        assert!(!report.execution_enabled_source);
    }

    fn run_report_with_plan(
        config_path: &Path,
        config: &AppConfig,
        route: &str,
        token: &str,
        notional_sol: f64,
        plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
    ) -> DevnetActivationDrillReport {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime");
        runtime
            .block_on(evaluate_devnet_activation_drill_with_plan(
                config_path,
                &execution_dry_run_rehearsal::resolve_db_path(config_path, &config.sqlite.path),
                config,
                route,
                token,
                notional_sol,
                plan,
            ))
            .expect("drill report")
    }

    fn ready_activation_plan_report() -> tiny_live_activation_plan::TinyLiveActivationPlanReport {
        let mut report = tiny_live_activation_plan_report(
            tiny_live_activation_plan::TinyLiveActivationPlanVerdict::BlockedByPreActivationGate,
            "pre-activation gate is not green yet: stage3 still accumulating",
        );
        report.pre_activation_gate.planning_green = false;
        report.pre_activation_gate.verdict = "blocked_by_stage3".to_string();
        report.pre_activation_gate.reason = "stage3 still accumulating".to_string();
        report
    }

    fn tiny_live_activation_plan_report(
        verdict: tiny_live_activation_plan::TinyLiveActivationPlanVerdict,
        reason: &str,
    ) -> tiny_live_activation_plan::TinyLiveActivationPlanReport {
        tiny_live_activation_plan::TinyLiveActivationPlanReport {
            generated_at: ts("2026-03-26T12:00:00Z"),
            config_path: "/tmp/devnet.server.toml".to_string(),
            output_path: None,
            planning_safe_only: true,
            activation_permission_granted: false,
            execution_enabled_current: false,
            current_execution_mode: "adapter_submit_confirm".to_string(),
            verdict,
            reason: reason.to_string(),
            blockers: Vec::new(),
            drift_findings: Vec::new(),
            pre_activation_gate: tiny_live_activation_plan::ActivationPlanGateSummary {
                verdict: "pre_activation_gates_green".to_string(),
                reason: "green".to_string(),
                planning_green: true,
                blockers: Vec::new(),
                execution_enabled: false,
                stage3_is_primary_gate: true,
            },
            tiny_live_policy: tiny_live_activation_plan::PolicyAuditSummary {
                verdict: "tiny_live_policy_bounded".to_string(),
                reason: "bounded".to_string(),
                bounded: true,
                tiny_live_policy_enabled: true,
                blocker_count: 0,
                first_blocker: None,
                warnings_count: 0,
            },
            tiny_live_guardrails: tiny_live_activation_plan::GuardrailAuditSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "guardrails bounded".to_string(),
                bounded: true,
                tiny_live_guardrails_enabled: true,
                first_blocker: None,
                blocker_count: 0,
                warnings_count: 0,
                rollback_triggers: Vec::new(),
            },
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            activation_overlay_change_count: 16,
            rollback_overlay_change_count: 16,
            activation_overlay_changes: vec![
                field_delta("execution.enabled", json!(false), json!(true)),
                field_delta(
                    "execution.mode",
                    json!("adapter_submit_confirm"),
                    json!("adapter_submit_confirm"),
                ),
                field_delta("execution.default_route", json!("jito"), json!("jito")),
                field_delta(
                    "execution.submit_allowed_routes",
                    json!(["jito"]),
                    json!(["jito"]),
                ),
                field_delta(
                    "execution.submit_route_order",
                    json!(["jito"]),
                    json!(["jito"]),
                ),
                field_delta(
                    "execution.submit_adapter_require_policy_echo",
                    json!(true),
                    json!(true),
                ),
                field_delta("shadow.copy_notional_sol", json!(0.05), json!(0.05)),
                field_delta("risk.max_position_sol", json!(0.05), json!(0.05)),
                field_delta("execution.batch_size", json!(1), json!(1)),
                field_delta("risk.max_concurrent_positions", json!(1), json!(1)),
                field_delta("risk.daily_loss_limit_pct", json!(1.0), json!(1.0)),
                field_delta(
                    "execution.pretrade_max_fee_overhead_bps",
                    json!(1000),
                    json!(1000),
                ),
                field_delta(
                    "execution.pretrade_max_priority_fee_lamports",
                    json!(2000),
                    json!(2000),
                ),
                field_delta(
                    "execution.submit_route_max_slippage_bps",
                    json!({"jito": 50.0}),
                    json!({"jito": 50.0}),
                ),
                field_delta(
                    "execution.submit_route_tip_lamports",
                    json!({"jito": 10000}),
                    json!({"jito": 10000}),
                ),
                field_delta(
                    "execution.submit_route_compute_unit_price_micro_lamports",
                    json!({"jito": 1500}),
                    json!({"jito": 1500}),
                ),
                field_delta(
                    "execution.submit_route_compute_unit_limit",
                    json!({"jito": 300000}),
                    json!({"jito": 300000}),
                ),
            ],
            unchanged_fields: vec![
                tiny_live_activation_plan::UnchangedFieldSummary {
                    field: "execution.rpc_http_url".to_string(),
                    value: json!("https://api.mainnet-beta.solana.com"),
                    reason: "reused".to_string(),
                },
                tiny_live_activation_plan::UnchangedFieldSummary {
                    field: "execution.submit_adapter_http_url".to_string(),
                    value: json!("https://prod.adapter.example.com/submit"),
                    reason: "reused".to_string(),
                },
            ],
            effective_tiny_live_contract: Some(
                tiny_live_activation_plan::EffectiveTinyLiveContract {
                    execution_mode: "adapter_submit_confirm".to_string(),
                    execution_enabled_target: true,
                    default_route: "jito".to_string(),
                    allowed_routes: vec!["jito".to_string()],
                    route_order: vec!["jito".to_string()],
                    policy_echo_required: true,
                    shadow_copy_notional_sol: 0.05,
                    risk_max_position_sol: 0.05,
                    execution_batch_size: 1,
                    risk_max_concurrent_positions: 1,
                    risk_daily_loss_limit_pct: 1.0,
                    pretrade_max_fee_overhead_bps: 1000,
                    pretrade_max_priority_fee_lamports: 2000,
                    route_envelope: vec![tiny_live_activation_plan::RouteEnvelopeRow {
                        route: "jito".to_string(),
                        compute_unit_limit: 300_000,
                        slippage_bps: 50.0,
                        tip_lamports: 10_000,
                        compute_unit_price_micro_lamports: 1_500,
                    }],
                },
            ),
            service_restart_contract: tiny_live_activation_plan::ServiceRestartContract {
                complete: true,
                activation_restart_required: true,
                rollback_restart_required: true,
                activation_services: vec!["solana-copy-bot.service".to_string()],
                rollback_services: vec!["solana-copy-bot.service".to_string()],
                activation_steps: vec!["restart".to_string()],
                rollback_steps: vec!["restart".to_string()],
            },
        }
    }

    fn field_delta(
        field: &str,
        current_value: Value,
        activation_value: Value,
    ) -> tiny_live_activation_plan::ActivationPlanFieldDelta {
        tiny_live_activation_plan::ActivationPlanFieldDelta {
            field: field.to_string(),
            current_value: current_value.clone(),
            activation_value,
            rollback_value: current_value,
            reason: "test".to_string(),
            source: "test".to_string(),
        }
    }

    fn base_devnet_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "paper-devnet".to_string();
        config.execution.enabled = false;
        config.execution.mode = "adapter_submit_confirm".to_string();
        config.execution.default_route = "jito".to_string();
        config.execution.rpc_http_url = "https://api.mainnet-beta.solana.com".to_string();
        config.execution.rpc_devnet_http_url = "http://127.0.0.1:8899".to_string();
        config.execution.submit_adapter_http_url = "http://127.0.0.1:8080/submit".to_string();
        config.execution.submit_adapter_devnet_http_url =
            "http://127.0.0.1:18080/submit".to_string();
        config.execution.submit_adapter_contract_version = "v1".to_string();
        config.execution.submit_adapter_require_policy_echo = true;
        config.execution.submit_allowed_routes = vec!["jito".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 50.0)].into_iter().collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000)].into_iter().collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config.execution.execution_signer_pubkey = "11111111111111111111111111111111".to_string();
        config.execution.submit_adapter_auth_token = "adapter-token".to_string();
        config.execution.batch_size = 1;
        config.execution.pretrade_max_fee_overhead_bps = 1_000;
        config.execution.pretrade_max_priority_fee_lamports = 2_000;

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
            [("jito".to_string(), 10_000u64)].into_iter().collect();
        config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500u64)].into_iter().collect();

        config.tiny_live_guardrails.enabled = true;
        config.tiny_live_guardrails.evaluation_window_seconds = 900;
        config.tiny_live_guardrails.max_execution_error_rate_pct = 5.0;
        config
            .tiny_live_guardrails
            .max_adapter_contract_failure_rate_pct = 1.0;
        config
            .tiny_live_guardrails
            .max_policy_echo_mismatch_rate_pct = 1.0;
        config
            .tiny_live_guardrails
            .max_fee_or_slippage_breach_rate_pct = 5.0;
        config
            .tiny_live_guardrails
            .max_connectivity_degraded_window_seconds = 120;
        config.tiny_live_guardrails.max_daily_realized_loss_sol = 0.05;
        config.tiny_live_guardrails.max_consecutive_hard_failures = 3;

        config.shadow.copy_notional_sol = 0.05;
        config.risk.max_position_sol = 0.05;
        config.risk.max_concurrent_positions = 1;
        config.risk.daily_loss_limit_pct = 1.0;
        config
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid rfc3339")
            .with_timezone(&Utc)
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_devnet_activation_drill_{}_{}_{}",
            label,
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_placeholder_config(path: &Path) {
        fs::write(path, "[system]\nenv = \"paper-devnet\"\n").expect("write placeholder config");
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct CapturedRequest {
        body: String,
    }

    #[derive(Debug, Clone)]
    struct MockResponse {
        status: u16,
        body: String,
        content_type: String,
    }

    impl MockResponse {
        fn json(body: serde_json::Value) -> Self {
            Self {
                status: 200,
                body: body.to_string(),
                content_type: "application/json".to_string(),
            }
        }
    }

    #[allow(dead_code)]
    struct MockHttpServer {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn spawn(responses: Vec<MockResponse>, required_header: Option<&'static str>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            listener
                .set_nonblocking(false)
                .expect("set blocking listener");
            let addr = listener.local_addr().expect("local addr");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_handle = Arc::clone(&requests);
            let handle = thread::spawn(move || {
                for response in responses {
                    let (mut stream, _) = listener.accept().expect("accept connection");
                    let request = read_http_request(&mut stream);
                    if let Some(header) = required_header {
                        assert!(
                            request
                                .to_ascii_lowercase()
                                .contains(&format!("{}:", header.to_ascii_lowercase())),
                            "expected header {header} in request {request}"
                        );
                    }
                    requests_handle
                        .lock()
                        .expect("lock requests")
                        .push(CapturedRequest {
                            body: request.clone(),
                        });
                    write_http_response(&mut stream, &response);
                }
            });
            Self {
                addr,
                requests,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}:{}{}", self.addr.ip(), self.addr.port(), path)
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut headers = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).expect("read request");
            if read == 0 {
                break;
            }
            headers.extend_from_slice(&buf[..read]);
            if headers.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let header_end = headers
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .expect("header terminator");
        let header_text = String::from_utf8_lossy(&headers[..header_end]).to_string();
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let lower = line.to_ascii_lowercase();
                lower
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>().expect("content length"))
            })
            .unwrap_or(0);
        let mut body = headers[header_end..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut buf).expect("read request body");
            if read == 0 {
                break;
            }
            body.extend_from_slice(&buf[..read]);
        }
        format!("{}{}", header_text, String::from_utf8_lossy(&body))
    }

    fn write_http_response(stream: &mut TcpStream, response: &MockResponse) {
        let body = response.body.as_bytes();
        let head = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.status,
            response.content_type,
            body.len()
        );
        stream.write_all(head.as_bytes()).expect("write head");
        stream.write_all(body).expect("write body");
        stream.flush().expect("flush response");
    }
}
