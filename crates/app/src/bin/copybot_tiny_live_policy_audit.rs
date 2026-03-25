use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig, ExecutionConfig, TinyLivePolicyConfig};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_policy_audit --config <path> [--json]";

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
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TinyLivePolicyVerdict {
    TinyLivePolicyBounded,
    TinyLivePolicyTooOpen,
    TinyLivePolicyIncomplete,
    TinyLivePolicyRouteRiskUnbounded,
    TinyLivePolicyFeeRiskUnbounded,
    TinyLivePolicyNotApplicableCurrentMode,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RoutePolicyAuditRow {
    pub(crate) route: String,
    pub(crate) current_allowed: bool,
    pub(crate) policy_allowed: bool,
    pub(crate) current_route_ordered: bool,
    pub(crate) current_slippage_bps: Option<f64>,
    pub(crate) policy_max_slippage_bps: Option<f64>,
    pub(crate) slippage_within_bound: bool,
    pub(crate) current_tip_lamports: Option<u64>,
    pub(crate) policy_max_tip_lamports: Option<u64>,
    pub(crate) tip_within_bound: bool,
    pub(crate) current_compute_unit_limit: Option<u32>,
    pub(crate) current_compute_unit_price_micro_lamports: Option<u64>,
    pub(crate) policy_max_compute_unit_price_micro_lamports: Option<u64>,
    pub(crate) compute_unit_price_within_bound: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TinyLivePolicyAuditReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) execution_enabled: bool,
    pub(crate) planning_safe_only: bool,
    pub(crate) stage3_gate_not_evaluated: bool,
    pub(crate) mode: String,
    pub(crate) mode_compatible: bool,
    pub(crate) execution_policy_contract_valid: bool,
    pub(crate) execution_route_contract_valid: bool,
    pub(crate) current_config_bounded_for_later_tiny_live_discussion: bool,
    pub(crate) suitable_only_for_paper_or_dry_run: bool,
    pub(crate) verdict: TinyLivePolicyVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) tiny_live_policy_enabled: bool,
    pub(crate) current_default_route: String,
    pub(crate) current_allowed_routes: Vec<String>,
    pub(crate) current_route_order: Vec<String>,
    pub(crate) policy_allowed_routes: Vec<String>,
    pub(crate) current_shadow_copy_notional_sol: f64,
    pub(crate) current_risk_max_position_sol: f64,
    pub(crate) policy_max_trade_notional_sol: f64,
    pub(crate) current_execution_batch_size: u32,
    pub(crate) policy_max_batch_size: u32,
    pub(crate) current_risk_max_concurrent_positions: u32,
    pub(crate) policy_max_concurrent_positions: u32,
    pub(crate) current_risk_daily_loss_limit_pct: f64,
    pub(crate) policy_max_daily_loss_limit_pct: f64,
    pub(crate) current_pretrade_max_fee_overhead_bps: u32,
    pub(crate) policy_max_pretrade_fee_overhead_bps: u32,
    pub(crate) current_pretrade_max_priority_fee_lamports: u64,
    pub(crate) policy_max_pretrade_priority_fee_lamports: u64,
    pub(crate) current_policy_echo_required: bool,
    pub(crate) policy_echo_required_for_tiny_live: bool,
    pub(crate) route_policy_rows: Vec<RoutePolicyAuditRow>,
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

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
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

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let report = evaluate_tiny_live_policy(&config.config_path, &loaded_config)?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing tiny-live policy audit json")
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) fn evaluate_tiny_live_policy(
    config_path: &Path,
    loaded_config: &AppConfig,
) -> Result<TinyLivePolicyAuditReport> {
    let mode = normalize_mode(loaded_config.execution.mode.as_str());
    let mode_compatible = is_supported_execution_mode(mode.as_str());
    let (
        execution_policy_contract_valid,
        policy_error,
        execution_route_contract_valid,
        route_error,
    ) = assess_execution_policy_shape(&loaded_config.execution);
    let policy = &loaded_config.tiny_live_policy;
    let current_default_route = normalize_route_or_empty(&loaded_config.execution.default_route);
    let current_allowed_routes =
        normalize_route_list(&loaded_config.execution.submit_allowed_routes);
    let current_route_order = normalize_route_list(&loaded_config.execution.submit_route_order);
    let policy_allowed_routes = normalize_route_list(&policy.allowed_routes);
    let route_policy_rows = build_route_policy_rows(loaded_config, policy);

    let mut incomplete_blockers = Vec::new();
    let mut route_blockers = Vec::new();
    let mut fee_blockers = Vec::new();
    let mut openness_blockers = Vec::new();
    let mut warnings = Vec::new();

    if mode != "adapter_submit_confirm" {
        incomplete_blockers.push(format!(
            "execution.mode={} is not the current tiny-live target mode; expected adapter_submit_confirm",
            if mode.is_empty() { "<empty>" } else { mode.as_str() }
        ));
    }
    if !policy.enabled {
        incomplete_blockers.push(
            "tiny_live_policy.enabled=false; explicit tiny-live policy contract is not configured"
                .to_string(),
        );
    }
    if !mode_compatible {
        incomplete_blockers.push(format!(
            "execution.mode={} is unsupported in the current runtime",
            if mode.is_empty() {
                "<empty>"
            } else {
                mode.as_str()
            }
        ));
    }
    if !execution_policy_contract_valid {
        incomplete_blockers.push(
            policy_error.unwrap_or_else(|| "execution policy contract incomplete".to_string()),
        );
    }
    if !execution_route_contract_valid {
        incomplete_blockers
            .push(route_error.unwrap_or_else(|| "execution route contract incomplete".to_string()));
    }
    if policy.enabled
        && policy.require_policy_echo
        && !loaded_config.execution.submit_adapter_require_policy_echo
    {
        incomplete_blockers.push(
            "execution.submit_adapter_require_policy_echo=false but tiny_live_policy.require_policy_echo=true"
                .to_string(),
        );
    }

    if policy.enabled {
        if !current_allowed_routes
            .iter()
            .all(|route| policy_allowed_routes.iter().any(|allowed| allowed == route))
        {
            let disallowed = current_allowed_routes
                .iter()
                .filter(|route| {
                    !policy_allowed_routes
                        .iter()
                        .any(|allowed| allowed == *route)
                })
                .cloned()
                .collect::<Vec<_>>();
            route_blockers.push(format!(
                "execution.submit_allowed_routes includes routes outside tiny_live_policy.allowed_routes: {}",
                disallowed.join(",")
            ));
        }
        if !current_route_order
            .iter()
            .all(|route| policy_allowed_routes.iter().any(|allowed| allowed == route))
        {
            let disallowed = current_route_order
                .iter()
                .filter(|route| {
                    !policy_allowed_routes
                        .iter()
                        .any(|allowed| allowed == *route)
                })
                .cloned()
                .collect::<Vec<_>>();
            route_blockers.push(format!(
                "execution.submit_route_order includes routes outside tiny_live_policy.allowed_routes: {}",
                disallowed.join(",")
            ));
        }
        if !current_default_route.is_empty()
            && !policy_allowed_routes
                .iter()
                .any(|route| route == &current_default_route)
        {
            route_blockers.push(format!(
                "execution.default_route={} is outside tiny_live_policy.allowed_routes",
                current_default_route
            ));
        }

        for row in &route_policy_rows {
            if row.current_allowed && !row.policy_allowed {
                route_blockers.push(format!(
                    "route={} is currently allowed but not allowed by tiny_live_policy",
                    row.route
                ));
            }
            if row.current_allowed && !row.slippage_within_bound {
                route_blockers.push(format!(
                    "execution.submit_route_max_slippage_bps[{}]={} exceeds tiny_live_policy.max_route_slippage_bps[{}]={}",
                    row.route,
                    display_option_f64(row.current_slippage_bps),
                    row.route,
                    display_option_f64(row.policy_max_slippage_bps),
                ));
            }
            if row.current_allowed && !row.tip_within_bound {
                fee_blockers.push(format!(
                    "execution.submit_route_tip_lamports[{}]={} exceeds tiny_live_policy.max_route_tip_lamports[{}]={}",
                    row.route,
                    display_option_u64(row.current_tip_lamports),
                    row.route,
                    display_option_u64(row.policy_max_tip_lamports),
                ));
            }
            if row.current_allowed && !row.compute_unit_price_within_bound {
                fee_blockers.push(format!(
                    "execution.submit_route_compute_unit_price_micro_lamports[{}]={} exceeds tiny_live_policy.max_route_compute_unit_price_micro_lamports[{}]={}",
                    row.route,
                    display_option_u64(row.current_compute_unit_price_micro_lamports),
                    row.route,
                    display_option_u64(row.policy_max_compute_unit_price_micro_lamports),
                ));
            }
        }

        if loaded_config.shadow.copy_notional_sol > policy.max_trade_notional_sol {
            openness_blockers.push(format!(
                "shadow.copy_notional_sol={:.6} exceeds tiny_live_policy.max_trade_notional_sol={:.6}",
                loaded_config.shadow.copy_notional_sol,
                policy.max_trade_notional_sol
            ));
        }
        if loaded_config.risk.max_position_sol > policy.max_trade_notional_sol {
            openness_blockers.push(format!(
                "risk.max_position_sol={:.6} exceeds tiny_live_policy.max_trade_notional_sol={:.6}",
                loaded_config.risk.max_position_sol, policy.max_trade_notional_sol
            ));
        }
        if loaded_config.execution.batch_size > policy.max_batch_size {
            openness_blockers.push(format!(
                "execution.batch_size={} exceeds tiny_live_policy.max_batch_size={}",
                loaded_config.execution.batch_size, policy.max_batch_size
            ));
        }
        if loaded_config.risk.max_concurrent_positions > policy.max_concurrent_positions {
            openness_blockers.push(format!(
                "risk.max_concurrent_positions={} exceeds tiny_live_policy.max_concurrent_positions={}",
                loaded_config.risk.max_concurrent_positions,
                policy.max_concurrent_positions
            ));
        }
        if loaded_config.risk.daily_loss_limit_pct > policy.max_daily_loss_limit_pct {
            openness_blockers.push(format!(
                "risk.daily_loss_limit_pct={:.6} exceeds tiny_live_policy.max_daily_loss_limit_pct={:.6}",
                loaded_config.risk.daily_loss_limit_pct,
                policy.max_daily_loss_limit_pct
            ));
        }
        if loaded_config.execution.pretrade_max_fee_overhead_bps
            > policy.max_pretrade_fee_overhead_bps
        {
            fee_blockers.push(format!(
                "execution.pretrade_max_fee_overhead_bps={} exceeds tiny_live_policy.max_pretrade_fee_overhead_bps={}",
                loaded_config.execution.pretrade_max_fee_overhead_bps,
                policy.max_pretrade_fee_overhead_bps
            ));
        }
        if loaded_config.execution.pretrade_max_priority_fee_lamports
            > policy.max_pretrade_priority_fee_lamports
        {
            fee_blockers.push(format!(
                "execution.pretrade_max_priority_fee_lamports={} exceeds tiny_live_policy.max_pretrade_priority_fee_lamports={}",
                loaded_config.execution.pretrade_max_priority_fee_lamports,
                policy.max_pretrade_priority_fee_lamports
            ));
        }
    }

    if !loaded_config.execution.enabled {
        warnings.push(
            "execution.enabled=false; tiny-live policy audit is planning-safe only".to_string(),
        );
    }
    warnings.push(
        "Stage 3 remains the gating truth; tiny-live policy boundedness does not authorize activation"
            .to_string(),
    );

    let verdict = if mode != "adapter_submit_confirm" || !mode_compatible {
        TinyLivePolicyVerdict::TinyLivePolicyNotApplicableCurrentMode
    } else if !incomplete_blockers.is_empty() {
        TinyLivePolicyVerdict::TinyLivePolicyIncomplete
    } else if !route_blockers.is_empty() {
        TinyLivePolicyVerdict::TinyLivePolicyRouteRiskUnbounded
    } else if !fee_blockers.is_empty() {
        TinyLivePolicyVerdict::TinyLivePolicyFeeRiskUnbounded
    } else if !openness_blockers.is_empty() {
        TinyLivePolicyVerdict::TinyLivePolicyTooOpen
    } else {
        TinyLivePolicyVerdict::TinyLivePolicyBounded
    };

    let mut blockers = Vec::new();
    blockers.extend(incomplete_blockers.clone());
    blockers.extend(route_blockers.clone());
    blockers.extend(fee_blockers.clone());
    blockers.extend(openness_blockers.clone());

    let reason = derive_reason(
        verdict,
        &incomplete_blockers,
        &route_blockers,
        &fee_blockers,
        &openness_blockers,
    );

    Ok(TinyLivePolicyAuditReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        execution_enabled: loaded_config.execution.enabled,
        planning_safe_only: true,
        stage3_gate_not_evaluated: true,
        mode,
        mode_compatible,
        execution_policy_contract_valid,
        execution_route_contract_valid,
        current_config_bounded_for_later_tiny_live_discussion: verdict
            == TinyLivePolicyVerdict::TinyLivePolicyBounded,
        suitable_only_for_paper_or_dry_run: verdict != TinyLivePolicyVerdict::TinyLivePolicyBounded,
        verdict,
        reason,
        blockers,
        warnings,
        tiny_live_policy_enabled: policy.enabled,
        current_default_route,
        current_allowed_routes,
        current_route_order,
        policy_allowed_routes,
        current_shadow_copy_notional_sol: loaded_config.shadow.copy_notional_sol,
        current_risk_max_position_sol: loaded_config.risk.max_position_sol,
        policy_max_trade_notional_sol: policy.max_trade_notional_sol,
        current_execution_batch_size: loaded_config.execution.batch_size,
        policy_max_batch_size: policy.max_batch_size,
        current_risk_max_concurrent_positions: loaded_config.risk.max_concurrent_positions,
        policy_max_concurrent_positions: policy.max_concurrent_positions,
        current_risk_daily_loss_limit_pct: loaded_config.risk.daily_loss_limit_pct,
        policy_max_daily_loss_limit_pct: policy.max_daily_loss_limit_pct,
        current_pretrade_max_fee_overhead_bps: loaded_config
            .execution
            .pretrade_max_fee_overhead_bps,
        policy_max_pretrade_fee_overhead_bps: policy.max_pretrade_fee_overhead_bps,
        current_pretrade_max_priority_fee_lamports: loaded_config
            .execution
            .pretrade_max_priority_fee_lamports,
        policy_max_pretrade_priority_fee_lamports: policy.max_pretrade_priority_fee_lamports,
        current_policy_echo_required: loaded_config.execution.submit_adapter_require_policy_echo,
        policy_echo_required_for_tiny_live: policy.require_policy_echo,
        route_policy_rows,
    })
}

fn assess_execution_policy_shape(
    execution: &ExecutionConfig,
) -> (bool, Option<String>, bool, Option<String>) {
    let default_route = normalize_route(&execution.default_route);
    let allowed_routes = normalize_route_list(&execution.submit_allowed_routes);
    let route_order = normalize_route_list(&execution.submit_route_order);

    if default_route.is_empty() {
        return (
            false,
            None,
            false,
            Some("execution.default_route cannot be empty for tiny-live policy audit".to_string()),
        );
    }

    if allowed_routes.is_empty() {
        return (
            false,
            None,
            false,
            Some(
                "execution.submit_allowed_routes must contain at least one route for tiny-live policy audit"
                    .to_string(),
            ),
        );
    }

    for route in &allowed_routes {
        if lookup_route_f64(&execution.submit_route_max_slippage_bps, route.as_str()).is_none() {
            return (
                true,
                None,
                false,
                Some(format!(
                    "execution.submit_route_max_slippage_bps is missing cap for allowed route={route}"
                )),
            );
        }
        if lookup_route_u64(&execution.submit_route_tip_lamports, route.as_str()).is_none() {
            return (
                true,
                None,
                false,
                Some(format!(
                    "execution.submit_route_tip_lamports is missing tip for allowed route={route}"
                )),
            );
        }
        if lookup_route_u32(&execution.submit_route_compute_unit_limit, route.as_str()).is_none() {
            return (
                true,
                None,
                false,
                Some(format!(
                    "execution.submit_route_compute_unit_limit is missing limit for allowed route={route}"
                )),
            );
        }
        if lookup_route_u64(
            &execution.submit_route_compute_unit_price_micro_lamports,
            route.as_str(),
        )
        .is_none()
        {
            return (
                true,
                None,
                false,
                Some(format!(
                    "execution.submit_route_compute_unit_price_micro_lamports is missing price for allowed route={route}"
                )),
            );
        }
    }

    if lookup_route_f64(
        &execution.submit_route_max_slippage_bps,
        default_route.as_str(),
    )
    .is_none()
    {
        return (
            true,
            None,
            false,
            Some(format!(
                "execution.submit_route_max_slippage_bps is missing cap for default route={default_route}"
            )),
        );
    }
    if lookup_route_u64(&execution.submit_route_tip_lamports, default_route.as_str()).is_none() {
        return (
            true,
            None,
            false,
            Some(format!(
                "execution.submit_route_tip_lamports is missing tip for default route={default_route}"
            )),
        );
    }
    if lookup_route_u32(
        &execution.submit_route_compute_unit_limit,
        default_route.as_str(),
    )
    .is_none()
    {
        return (
            true,
            None,
            false,
            Some(format!(
                "execution.submit_route_compute_unit_limit is missing limit for default route={default_route}"
            )),
        );
    }
    if lookup_route_u64(
        &execution.submit_route_compute_unit_price_micro_lamports,
        default_route.as_str(),
    )
    .is_none()
    {
        return (
            true,
            None,
            false,
            Some(format!(
                "execution.submit_route_compute_unit_price_micro_lamports is missing price for default route={default_route}"
            )),
        );
    }

    if !route_order.is_empty()
        && !route_order
            .iter()
            .all(|route| allowed_routes.iter().any(|allowed| allowed == route))
    {
        let disallowed = route_order
            .iter()
            .filter(|route| !allowed_routes.iter().any(|allowed| allowed == *route))
            .cloned()
            .collect::<Vec<_>>();
        return (
            false,
            Some(format!(
                "execution.submit_route_order includes routes outside execution.submit_allowed_routes: {}",
                disallowed.join(",")
            )),
            true,
            None,
        );
    }

    (true, None, true, None)
}

fn build_route_policy_rows(
    loaded_config: &AppConfig,
    policy: &TinyLivePolicyConfig,
) -> Vec<RoutePolicyAuditRow> {
    let mut routes = BTreeSet::new();
    for route in &loaded_config.execution.submit_allowed_routes {
        let normalized = normalize_route(route);
        if !normalized.is_empty() {
            routes.insert(normalized);
        }
    }
    for route in &loaded_config.execution.submit_route_order {
        let normalized = normalize_route(route);
        if !normalized.is_empty() {
            routes.insert(normalized);
        }
    }
    let default_route = normalize_route(&loaded_config.execution.default_route);
    if !default_route.is_empty() {
        routes.insert(default_route);
    }
    for route in &policy.allowed_routes {
        let normalized = normalize_route(route);
        if !normalized.is_empty() {
            routes.insert(normalized);
        }
    }

    routes
        .into_iter()
        .map(|route| {
            let current_slippage = lookup_route_f64(
                &loaded_config.execution.submit_route_max_slippage_bps,
                route.as_str(),
            );
            let policy_slippage = lookup_route_f64(&policy.max_route_slippage_bps, route.as_str());
            let current_tip = lookup_route_u64(
                &loaded_config.execution.submit_route_tip_lamports,
                route.as_str(),
            );
            let policy_tip = lookup_route_u64(&policy.max_route_tip_lamports, route.as_str());
            let current_cu_limit = lookup_route_u32(
                &loaded_config.execution.submit_route_compute_unit_limit,
                route.as_str(),
            );
            let current_cu_price = lookup_route_u64(
                &loaded_config
                    .execution
                    .submit_route_compute_unit_price_micro_lamports,
                route.as_str(),
            );
            let policy_cu_price = lookup_route_u64(
                &policy.max_route_compute_unit_price_micro_lamports,
                route.as_str(),
            );
            RoutePolicyAuditRow {
                current_allowed: loaded_config
                    .execution
                    .submit_allowed_routes
                    .iter()
                    .any(|candidate| normalize_route(candidate) == route),
                policy_allowed: policy
                    .allowed_routes
                    .iter()
                    .any(|candidate| normalize_route(candidate) == route),
                current_route_ordered: loaded_config
                    .execution
                    .submit_route_order
                    .iter()
                    .any(|candidate| normalize_route(candidate) == route),
                route,
                current_slippage_bps: current_slippage,
                policy_max_slippage_bps: policy_slippage,
                slippage_within_bound: compare_opt_f64_le(current_slippage, policy_slippage),
                current_tip_lamports: current_tip,
                policy_max_tip_lamports: policy_tip,
                tip_within_bound: compare_opt_u64_le(current_tip, policy_tip),
                current_compute_unit_limit: current_cu_limit,
                current_compute_unit_price_micro_lamports: current_cu_price,
                policy_max_compute_unit_price_micro_lamports: policy_cu_price,
                compute_unit_price_within_bound: compare_opt_u64_le(
                    current_cu_price,
                    policy_cu_price,
                ),
            }
        })
        .collect()
}

fn derive_reason(
    verdict: TinyLivePolicyVerdict,
    incomplete_blockers: &[String],
    route_blockers: &[String],
    fee_blockers: &[String],
    openness_blockers: &[String],
) -> String {
    match verdict {
        TinyLivePolicyVerdict::TinyLivePolicyBounded => {
            "current execution/risk envelope is explicitly bounded enough for later tiny-live discussion; execution.enabled remains false".to_string()
        }
        TinyLivePolicyVerdict::TinyLivePolicyTooOpen => openness_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "current execution/risk envelope is too open for tiny-live".to_string()),
        TinyLivePolicyVerdict::TinyLivePolicyIncomplete => incomplete_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "tiny-live policy contract is incomplete".to_string()),
        TinyLivePolicyVerdict::TinyLivePolicyRouteRiskUnbounded => route_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "route set or per-route slippage policy is unbounded for tiny-live".to_string()),
        TinyLivePolicyVerdict::TinyLivePolicyFeeRiskUnbounded => fee_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "fee/tip/priority-fee envelope is unbounded for tiny-live".to_string()),
        TinyLivePolicyVerdict::TinyLivePolicyNotApplicableCurrentMode => {
            "current execution mode is not the adapter-submit tiny-live target mode".to_string()
        }
    }
}

fn normalize_route_or_empty(value: &str) -> String {
    normalize_route(value)
}

fn normalize_route_list(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| normalize_route(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn normalize_route(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_mode(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn is_supported_execution_mode(mode: &str) -> bool {
    matches!(
        mode,
        "paper" | "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    )
}

fn lookup_route_f64(map: &std::collections::BTreeMap<String, f64>, route: &str) -> Option<f64> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn lookup_route_u64(map: &std::collections::BTreeMap<String, u64>, route: &str) -> Option<u64> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn lookup_route_u32(map: &std::collections::BTreeMap<String, u32>, route: &str) -> Option<u32> {
    map.iter()
        .find(|(candidate, _)| candidate.trim().eq_ignore_ascii_case(route))
        .map(|(_, value)| *value)
}

fn compare_opt_f64_le(current: Option<f64>, bound: Option<f64>) -> bool {
    match (current, bound) {
        (Some(current), Some(bound)) => current <= bound,
        _ => false,
    }
}

fn compare_opt_u64_le(current: Option<u64>, bound: Option<u64>) -> bool {
    match (current, bound) {
        (Some(current), Some(bound)) => current <= bound,
        _ => false,
    }
}

fn display_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.6}"))
        .unwrap_or_else(|| "null".to_string())
}

fn display_option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

pub(crate) fn render_human(report: &TinyLivePolicyAuditReport) -> String {
    [
        "event=copybot_tiny_live_policy_audit".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("execution_enabled={}", report.execution_enabled),
        format!("planning_safe_only={}", report.planning_safe_only),
        format!(
            "stage3_gate_not_evaluated={}",
            report.stage3_gate_not_evaluated
        ),
        format!("mode={}", report.mode),
        format!("mode_compatible={}", report.mode_compatible),
        format!(
            "verdict={}",
            serde_json::to_string(&report.verdict)
                .unwrap_or_default()
                .trim_matches('"')
        ),
        format!("reason={}", report.reason),
        format!(
            "current_config_bounded_for_later_tiny_live_discussion={}",
            report.current_config_bounded_for_later_tiny_live_discussion
        ),
        format!(
            "suitable_only_for_paper_or_dry_run={}",
            report.suitable_only_for_paper_or_dry_run
        ),
        format!(
            "tiny_live_policy_enabled={}",
            report.tiny_live_policy_enabled
        ),
        format!(
            "execution_policy_contract_valid={}",
            report.execution_policy_contract_valid
        ),
        format!(
            "execution_route_contract_valid={}",
            report.execution_route_contract_valid
        ),
        format!("current_default_route={}", report.current_default_route),
        format!(
            "current_allowed_routes={}",
            report.current_allowed_routes.join(",")
        ),
        format!(
            "current_route_order={}",
            report.current_route_order.join(",")
        ),
        format!(
            "policy_allowed_routes={}",
            report.policy_allowed_routes.join(",")
        ),
        format!(
            "current_shadow_copy_notional_sol={:.6}",
            report.current_shadow_copy_notional_sol
        ),
        format!(
            "current_risk_max_position_sol={:.6}",
            report.current_risk_max_position_sol
        ),
        format!(
            "policy_max_trade_notional_sol={:.6}",
            report.policy_max_trade_notional_sol
        ),
        format!(
            "current_execution_batch_size={}",
            report.current_execution_batch_size
        ),
        format!("policy_max_batch_size={}", report.policy_max_batch_size),
        format!(
            "current_risk_max_concurrent_positions={}",
            report.current_risk_max_concurrent_positions
        ),
        format!(
            "policy_max_concurrent_positions={}",
            report.policy_max_concurrent_positions
        ),
        format!(
            "current_risk_daily_loss_limit_pct={:.6}",
            report.current_risk_daily_loss_limit_pct
        ),
        format!(
            "policy_max_daily_loss_limit_pct={:.6}",
            report.policy_max_daily_loss_limit_pct
        ),
        format!(
            "current_pretrade_max_fee_overhead_bps={}",
            report.current_pretrade_max_fee_overhead_bps
        ),
        format!(
            "policy_max_pretrade_fee_overhead_bps={}",
            report.policy_max_pretrade_fee_overhead_bps
        ),
        format!(
            "current_pretrade_max_priority_fee_lamports={}",
            report.current_pretrade_max_priority_fee_lamports
        ),
        format!(
            "policy_max_pretrade_priority_fee_lamports={}",
            report.policy_max_pretrade_priority_fee_lamports
        ),
        format!(
            "current_policy_echo_required={}",
            report.current_policy_echo_required
        ),
        format!(
            "policy_echo_required_for_tiny_live={}",
            report.policy_echo_required_for_tiny_live
        ),
        format!(
            "route_policy_rows={}",
            report
                .route_policy_rows
                .iter()
                .map(render_route_row_human)
                .collect::<Vec<_>>()
                .join(" ; ")
        ),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("warnings={}", report.warnings.join(" | ")),
    ]
    .join("\n")
}

fn render_route_row_human(row: &RoutePolicyAuditRow) -> String {
    format!(
        "{}(current_allowed={},policy_allowed={},ordered={},slippage={}/{},tip={}/{},cu_limit={},cu_price={}/{})",
        row.route,
        row.current_allowed,
        row.policy_allowed,
        row.current_route_ordered,
        display_option_f64(row.current_slippage_bps),
        display_option_f64(row.policy_max_slippage_bps),
        display_option_u64(row.current_tip_lamports),
        display_option_u64(row.policy_max_tip_lamports),
        row.current_compute_unit_limit
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        display_option_u64(row.current_compute_unit_price_micro_lamports),
        display_option_u64(row.policy_max_compute_unit_price_micro_lamports),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::AppConfig;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bounded_tiny_live_policy_returns_green() {
        let temp = temp_dir("tiny_live_policy_green");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let config = bounded_config();
        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert_eq!(report.verdict, TinyLivePolicyVerdict::TinyLivePolicyBounded);
        assert!(report.current_config_bounded_for_later_tiny_live_discussion);
        assert!(!report.execution_enabled);
    }

    #[test]
    fn oversized_trade_notional_blocks_policy() {
        let temp = temp_dir("tiny_live_policy_notional_blocked");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = bounded_config();
        config.shadow.copy_notional_sol = 0.15;

        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert_eq!(report.verdict, TinyLivePolicyVerdict::TinyLivePolicyTooOpen);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("shadow.copy_notional_sol")));
    }

    #[test]
    fn unbounded_route_set_blocks_policy() {
        let temp = temp_dir("tiny_live_policy_route_unbounded");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = bounded_config();
        config.execution.submit_allowed_routes = vec!["jito".to_string(), "rpc".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string(), "rpc".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 40.0), ("rpc".to_string(), 40.0)]
                .into_iter()
                .collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000), ("rpc".to_string(), 0)]
                .into_iter()
                .collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000), ("rpc".to_string(), 300_000)]
                .into_iter()
                .collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500), ("rpc".to_string(), 1_000)]
                .into_iter()
                .collect();

        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert_eq!(
            report.verdict,
            TinyLivePolicyVerdict::TinyLivePolicyRouteRiskUnbounded
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("submit_allowed_routes")));
    }

    #[test]
    fn fee_envelope_outside_policy_blocks_policy() {
        let temp = temp_dir("tiny_live_policy_fee_unbounded");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = bounded_config();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 20_000)].into_iter().collect();

        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert_eq!(
            report.verdict,
            TinyLivePolicyVerdict::TinyLivePolicyFeeRiskUnbounded
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("submit_route_tip_lamports")));
    }

    #[test]
    fn missing_policy_block_is_reported_as_incomplete() {
        let temp = temp_dir("tiny_live_policy_incomplete");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let mut config = base_adapter_config();
        config.tiny_live_policy.enabled = false;

        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert_eq!(
            report.verdict,
            TinyLivePolicyVerdict::TinyLivePolicyIncomplete
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("tiny_live_policy.enabled=false")));
    }

    #[test]
    fn stage3_truth_is_not_required_for_policy_boundedness() {
        let temp = temp_dir("tiny_live_policy_stage3_not_used");
        let config_path = temp.join("live.server.toml");
        write_placeholder_config(&config_path);

        let config = bounded_config();
        let report = evaluate_tiny_live_policy(&config_path, &config).expect("policy audit");

        assert!(report.stage3_gate_not_evaluated);
        assert_eq!(report.verdict, TinyLivePolicyVerdict::TinyLivePolicyBounded);
    }

    fn bounded_config() -> AppConfig {
        let mut config = base_adapter_config();
        config.shadow.copy_notional_sol = 0.05;
        config.risk.max_position_sol = 0.05;
        config.risk.max_concurrent_positions = 1;
        config.risk.daily_loss_limit_pct = 0.75;
        config.execution.batch_size = 1;
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
        config.execution.pretrade_max_fee_overhead_bps = 800;
        config.execution.pretrade_max_priority_fee_lamports = 1_500;
        config.execution.submit_adapter_require_policy_echo = true;
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

    fn base_adapter_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "paper".to_string();
        config.execution.enabled = false;
        config.execution.mode = "adapter_submit_confirm".to_string();
        config.execution.default_route = "jito".to_string();
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
        config.execution.submit_adapter_require_policy_echo = true;
        config
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_tiny_live_policy_audit_{}_{}_{}",
            label,
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write_placeholder_config(path: &Path) {
        fs::write(path, "").expect("write placeholder config");
    }
}
