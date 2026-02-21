use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, HashSet};

use super::ExecutionConfig;

pub(crate) fn parse_env_bool(value: String) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub(crate) fn validate_adapter_route_policy_completeness(config: &ExecutionConfig) -> Result<()> {
    if !config.enabled
        || !config
            .mode
            .trim()
            .eq_ignore_ascii_case("adapter_submit_confirm")
    {
        return Ok(());
    }

    let default_route = {
        let value = config.default_route.trim().to_ascii_lowercase();
        if value.is_empty() {
            "paper".to_string()
        } else {
            value
        }
    };

    let allowed_routes: Vec<String> = config
        .submit_allowed_routes
        .iter()
        .map(|route| route.trim().to_ascii_lowercase())
        .filter(|route| !route.is_empty())
        .collect();

    for route in &allowed_routes {
        if !map_contains_route(&config.submit_route_max_slippage_bps, route) {
            return Err(anyhow!(
                "execution.submit_route_max_slippage_bps is missing cap for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS format route:cap)",
                route
            ));
        }
        if !map_contains_route(&config.submit_route_tip_lamports, route) {
            return Err(anyhow!(
                "execution.submit_route_tip_lamports is missing tip for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS format route:tip)",
                route
            ));
        }
        if !map_contains_route(&config.submit_route_compute_unit_limit, route) {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit is missing limit for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT format route:limit)",
                route
            ));
        }
        if !map_contains_route(
            &config.submit_route_compute_unit_price_micro_lamports,
            route,
        ) {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports is missing price for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS format route:price)",
                route
            ));
        }
    }

    if !map_contains_route(
        &config.submit_route_max_slippage_bps,
        default_route.as_str(),
    ) {
        return Err(anyhow!(
            "execution.submit_route_max_slippage_bps is missing cap for default route={}",
            default_route
        ));
    }
    if !map_contains_route(&config.submit_route_tip_lamports, default_route.as_str()) {
        return Err(anyhow!(
            "execution.submit_route_tip_lamports is missing tip for default route={}",
            default_route
        ));
    }
    if !map_contains_route(
        &config.submit_route_compute_unit_limit,
        default_route.as_str(),
    ) {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_limit is missing limit for default route={}",
            default_route
        ));
    }
    if !map_contains_route(
        &config.submit_route_compute_unit_price_micro_lamports,
        default_route.as_str(),
    ) {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_price_micro_lamports is missing price for default route={}",
            default_route
        ));
    }

    Ok(())
}

fn map_contains_route<T>(map: &BTreeMap<String, T>, route: &str) -> bool {
    map.keys()
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(route))
}

pub(crate) fn parse_execution_route_map_env<T, F>(
    csv: &str,
    env_name: &str,
    parse_value: F,
) -> Result<BTreeMap<String, T>>
where
    F: Fn(&str) -> Option<T>,
{
    let mut values = BTreeMap::new();
    let mut seen_normalized = HashSet::new();
    for token in csv.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let Some((route, raw_value)) = token.split_once(':') else {
            return Err(anyhow!(
                "{env_name} contains malformed token (expected route:value): {}",
                token
            ));
        };
        let route = route.trim().to_ascii_lowercase();
        if route.is_empty() {
            return Err(anyhow!(
                "{env_name} contains empty route key in token: {}",
                token
            ));
        }
        let Some(parsed_value) = parse_value(raw_value) else {
            return Err(anyhow!(
                "{env_name} contains invalid numeric value for route={}: {}",
                route,
                raw_value.trim()
            ));
        };
        if !seen_normalized.insert(route.clone()) {
            return Err(anyhow!(
                "{env_name} contains duplicate route after normalization: {}",
                route
            ));
        }
        values.insert(route, parsed_value);
    }
    Ok(values)
}

pub(crate) fn parse_execution_route_list_env(csv: &str, env_name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut seen_normalized = HashSet::new();
    for token in csv.split(',') {
        let route = token.trim();
        if route.is_empty() {
            continue;
        }
        let normalized = route.to_ascii_lowercase();
        if !seen_normalized.insert(normalized.clone()) {
            return Err(anyhow!(
                "{env_name} contains duplicate route after normalization: {}",
                normalized
            ));
        }
        values.push(route.to_string());
    }
    Ok(values)
}
