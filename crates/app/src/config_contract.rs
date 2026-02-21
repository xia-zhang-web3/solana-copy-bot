use anyhow::{anyhow, Context, Result};
use copybot_config::{
    ExecutionConfig, EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX,
    EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN, EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
    EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN,
    EXECUTION_ROUTE_DEFAULT_COMPUTE_UNIT_LIMIT_MIN, EXECUTION_ROUTE_TIP_LAMPORTS_MAX,
};
use std::collections::{BTreeMap, HashSet};
use tracing::info;
use url::{Host, Url};

pub(crate) fn contains_placeholder_value(value: &str) -> bool {
    value.to_ascii_uppercase().contains("REPLACE_ME")
}

pub(crate) fn validate_execution_runtime_contract(
    config: &ExecutionConfig,
    env: &str,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    let mode = validate_execution_mode_contract(config)?;
    validate_signer_contract(config, mode.as_str())?;
    validate_adapter_contract(config, env, mode.as_str())?;
    validate_routes_contract(config, mode.as_str())?;

    if matches!(
        env.trim().to_ascii_lowercase().as_str(),
        "paper" | "prod" | "paper-canary" | "paper-canary-yellowstone"
    ) {
        info!(
            mode,
            batch_size = config.batch_size,
            poll_interval_ms = config.poll_interval_ms,
            submit_timeout_ms = config.submit_timeout_ms,
            submit_allowed_routes = ?config.submit_allowed_routes,
            submit_route_max_slippage_bps = ?config.submit_route_max_slippage_bps,
            submit_route_tip_lamports = ?config.submit_route_tip_lamports,
            submit_route_compute_unit_limit = ?config.submit_route_compute_unit_limit,
            submit_route_compute_unit_price_micro_lamports = ?config.submit_route_compute_unit_price_micro_lamports,
            submit_dynamic_cu_price_enabled = config.submit_dynamic_cu_price_enabled,
            submit_dynamic_cu_price_percentile = config.submit_dynamic_cu_price_percentile,
            submit_dynamic_tip_lamports_enabled = config.submit_dynamic_tip_lamports_enabled,
            submit_dynamic_tip_lamports_multiplier_bps = config.submit_dynamic_tip_lamports_multiplier_bps,
            submit_adapter_contract_version = %config.submit_adapter_contract_version,
            submit_adapter_require_policy_echo = config.submit_adapter_require_policy_echo,
            pretrade_require_token_account = config.pretrade_require_token_account,
            pretrade_max_priority_fee_micro_lamports_per_cu = config.pretrade_max_priority_fee_lamports,
            "execution runtime contract validated"
        );
    }

    Ok(())
}

fn validate_execution_mode_contract(config: &ExecutionConfig) -> Result<String> {
    let mode = config.mode.trim().to_ascii_lowercase();
    if !matches!(
        mode.as_str(),
        "paper" | "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) {
        return Err(anyhow!(
            "execution.enabled=true but execution.mode={} is unsupported in current runtime; supported modes: paper, paper_rpc_confirm, paper_rpc_pretrade_confirm, adapter_submit_confirm",
            if mode.is_empty() { "<empty>" } else { mode.as_str() }
        ));
    }
    if matches!(
        mode.as_str(),
        "paper_rpc_confirm" | "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) {
        let primary = config.rpc_http_url.trim();
        let fallback = config.rpc_fallback_http_url.trim();
        if primary.is_empty() && fallback.is_empty() {
            return Err(anyhow!(
                "execution.mode={} requires execution.rpc_http_url or execution.rpc_fallback_http_url",
                mode
            ));
        }
    }

    Ok(mode)
}

fn validate_signer_contract(config: &ExecutionConfig, mode: &str) -> Result<()> {
    if matches!(
        mode,
        "paper_rpc_pretrade_confirm" | "adapter_submit_confirm"
    ) && config.execution_signer_pubkey.trim().is_empty()
    {
        return Err(anyhow!(
            "execution.mode={} requires non-empty execution.execution_signer_pubkey",
            mode
        ));
    }

    Ok(())
}

fn validate_adapter_contract(config: &ExecutionConfig, env: &str, mode: &str) -> Result<()> {
    if mode != "adapter_submit_confirm" {
        return Ok(());
    }

    let submit_primary = config.submit_adapter_http_url.trim();
    let submit_fallback = config.submit_adapter_fallback_http_url.trim();
    if submit_primary.is_empty() && submit_fallback.is_empty() {
        return Err(anyhow!(
            "execution.mode=adapter_submit_confirm requires execution.submit_adapter_http_url or execution.submit_adapter_fallback_http_url"
        ));
    }
    if !submit_primary.is_empty() {
        validate_adapter_endpoint_url(
            submit_primary,
            "execution.submit_adapter_http_url",
            is_production_env_profile(env),
        )?;
    }
    if !submit_fallback.is_empty() {
        validate_adapter_endpoint_url(
            submit_fallback,
            "execution.submit_adapter_fallback_http_url",
            is_production_env_profile(env),
        )?;
    }
    if !submit_primary.is_empty() && !submit_fallback.is_empty() {
        let primary_identity = adapter_endpoint_identity(submit_primary).with_context(|| {
            "failed normalizing execution.submit_adapter_http_url endpoint identity"
        })?;
        let fallback_identity = adapter_endpoint_identity(submit_fallback).with_context(|| {
            "failed normalizing execution.submit_adapter_fallback_http_url endpoint identity"
        })?;
        if primary_identity == fallback_identity {
            return Err(anyhow!(
                "execution.submit_adapter_http_url and execution.submit_adapter_fallback_http_url must resolve to distinct endpoints when both are set"
            ));
        }
    }
    let hmac_key_id = config.submit_adapter_hmac_key_id.trim();
    let hmac_secret = config.submit_adapter_hmac_secret.trim();
    if hmac_key_id.is_empty() ^ hmac_secret.is_empty() {
        return Err(anyhow!(
            "execution.mode=adapter_submit_confirm requires execution.submit_adapter_hmac_key_id and execution.submit_adapter_hmac_secret to be both set or both empty"
        ));
    }
    if !hmac_key_id.is_empty() && !(5..=300).contains(&config.submit_adapter_hmac_ttl_sec) {
        return Err(anyhow!(
            "execution.submit_adapter_hmac_ttl_sec must be in 5..=300 when adapter HMAC auth is enabled"
        ));
    }
    let contract_version = config.submit_adapter_contract_version.trim();
    if contract_version.is_empty() {
        return Err(anyhow!(
            "execution.mode=adapter_submit_confirm requires non-empty execution.submit_adapter_contract_version"
        ));
    }
    if contract_version.len() > 64 {
        return Err(anyhow!(
            "execution.submit_adapter_contract_version must be <= 64 chars"
        ));
    }
    if !is_valid_contract_version_token(contract_version) {
        return Err(anyhow!(
            "execution.submit_adapter_contract_version must contain only [A-Za-z0-9._-]"
        ));
    }
    if is_production_env_profile(env) && !config.submit_adapter_require_policy_echo {
        return Err(anyhow!(
            "execution.submit_adapter_require_policy_echo must be true in production-like environments when execution.mode=adapter_submit_confirm"
        ));
    }

    Ok(())
}

fn validate_routes_contract(config: &ExecutionConfig, mode: &str) -> Result<()> {
    if config.batch_size == 0 {
        return Err(anyhow!(
            "execution.batch_size must be >= 1 when execution is enabled"
        ));
    }
    if config.poll_interval_ms < 100 {
        return Err(anyhow!(
            "execution.poll_interval_ms must be >= 100ms when execution is enabled"
        ));
    }
    if config.max_confirm_seconds == 0 {
        return Err(anyhow!(
            "execution.max_confirm_seconds must be >= 1 when execution is enabled"
        ));
    }
    if config.max_submit_attempts == 0 {
        return Err(anyhow!(
            "execution.max_submit_attempts must be >= 1 when execution is enabled"
        ));
    }
    if config.submit_dynamic_cu_price_percentile == 0
        || config.submit_dynamic_cu_price_percentile > 100
    {
        return Err(anyhow!(
            "execution.submit_dynamic_cu_price_percentile must be in 1..=100 when execution is enabled"
        ));
    }
    if config.submit_timeout_ms < 100 {
        return Err(anyhow!(
            "execution.submit_timeout_ms must be >= 100ms when execution is enabled"
        ));
    }
    if !config.pretrade_min_sol_reserve.is_finite() || config.pretrade_min_sol_reserve < 0.0 {
        return Err(anyhow!(
            "execution.pretrade_min_sol_reserve must be finite and >= 0 when execution is enabled"
        ));
    }
    if config.slippage_bps < 0.0 {
        return Err(anyhow!(
            "execution.slippage_bps must be >= 0 when execution is enabled"
        ));
    }
    if config.submit_allowed_routes.is_empty() {
        return Err(anyhow!(
            "execution.submit_allowed_routes must not be empty when execution is enabled"
        ));
    }
    validate_unique_normalized_route_list(
        &config.submit_allowed_routes,
        "execution.submit_allowed_routes",
    )?;
    let default_route = config.default_route.trim().to_ascii_lowercase();
    let default_route = if default_route.is_empty() {
        "paper".to_string()
    } else {
        default_route
    };
    let route_allowed = config
        .submit_allowed_routes
        .iter()
        .any(|route| route.trim().to_ascii_lowercase().eq(default_route.as_str()));
    if !route_allowed {
        return Err(anyhow!(
            "execution.default_route={} must be present in execution.submit_allowed_routes",
            default_route
        ));
    }
    if !config.submit_route_order.is_empty() {
        validate_unique_normalized_route_list(
            &config.submit_route_order,
            "execution.submit_route_order",
        )?;
        for route in &config.submit_route_order {
            let normalized = route.trim().to_ascii_lowercase();
            if normalized.is_empty() {
                return Err(anyhow!(
                    "execution.submit_route_order contains an empty route value"
                ));
            }
            let allowed = config.submit_allowed_routes.iter().any(|candidate| {
                candidate
                    .trim()
                    .to_ascii_lowercase()
                    .eq(normalized.as_str())
            });
            if !allowed {
                return Err(anyhow!(
                    "execution.submit_route_order route={} must be present in execution.submit_allowed_routes",
                    normalized
                ));
            }
        }
        let has_default = config
            .submit_route_order
            .iter()
            .any(|route| route.trim().to_ascii_lowercase().eq(default_route.as_str()));
        if !has_default {
            return Err(anyhow!(
                "execution.submit_route_order must include execution.default_route={}",
                default_route
            ));
        }
    }
    if config.submit_route_max_slippage_bps.is_empty() {
        return Err(anyhow!(
            "execution.submit_route_max_slippage_bps must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS=route:cap,route2:cap2)"
        ));
    }
    validate_unique_normalized_route_map_keys(
        &config.submit_route_max_slippage_bps,
        "execution.submit_route_max_slippage_bps",
    )?;
    for (route, cap) in &config.submit_route_max_slippage_bps {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_max_slippage_bps contains empty route key"
            ));
        }
        if !cap.is_finite() || *cap <= 0.0 {
            return Err(anyhow!(
                "execution.submit_route_max_slippage_bps route={} must be finite and > 0, got {}",
                route,
                cap
            ));
        }
    }
    if config.submit_route_tip_lamports.is_empty() {
        return Err(anyhow!(
            "execution.submit_route_tip_lamports must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS=route:tip,route2:tip2)"
        ));
    }
    validate_unique_normalized_route_map_keys(
        &config.submit_route_tip_lamports,
        "execution.submit_route_tip_lamports",
    )?;
    for (route, tip_lamports) in &config.submit_route_tip_lamports {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_tip_lamports contains empty route key"
            ));
        }
        if *tip_lamports > EXECUTION_ROUTE_TIP_LAMPORTS_MAX {
            return Err(anyhow!(
                "execution.submit_route_tip_lamports route={} must be in 0..={}, got {}",
                route,
                EXECUTION_ROUTE_TIP_LAMPORTS_MAX,
                tip_lamports
            ));
        }
    }
    if config.submit_route_compute_unit_limit.is_empty() {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_limit must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT=route:limit,route2:limit2)"
        ));
    }
    validate_unique_normalized_route_map_keys(
        &config.submit_route_compute_unit_limit,
        "execution.submit_route_compute_unit_limit",
    )?;
    for (route, limit) in &config.submit_route_compute_unit_limit {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit contains empty route key"
            ));
        }
        if *limit < EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN
            || *limit > EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX
        {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit route={} must be in {}..={}, got {}",
                route,
                EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN,
                EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX,
                limit
            ));
        }
    }
    if config
        .submit_route_compute_unit_price_micro_lamports
        .is_empty()
    {
        return Err(anyhow!(
            "execution.submit_route_compute_unit_price_micro_lamports must not be empty when execution is enabled (env format: SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS=route:price,route2:price2)"
        ));
    }
    validate_unique_normalized_route_map_keys(
        &config.submit_route_compute_unit_price_micro_lamports,
        "execution.submit_route_compute_unit_price_micro_lamports",
    )?;
    for (route, price) in &config.submit_route_compute_unit_price_micro_lamports {
        if route.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports contains empty route key"
            ));
        }
        if *price < EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN
            || *price > EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX
        {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_price_micro_lamports route={} must be in {}..={}, got {}",
                route,
                EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN,
                EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX,
                price
            ));
        }
    }
    if mode == "adapter_submit_confirm" {
        let find_route_cap = |route: &str| -> Option<f64> {
            config
                .submit_route_max_slippage_bps
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, cap)| *cap)
        };
        let find_route_tip = |route: &str| -> Option<u64> {
            config
                .submit_route_tip_lamports
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, tip)| *tip)
        };
        let find_route_cu_limit = |route: &str| -> Option<u32> {
            config
                .submit_route_compute_unit_limit
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, limit)| *limit)
        };
        let find_route_cu_price = |route: &str| -> Option<u64> {
            config
                .submit_route_compute_unit_price_micro_lamports
                .iter()
                .find(|(key, _)| key.trim().eq_ignore_ascii_case(route))
                .map(|(_, price)| *price)
        };
        for allowed_route in &config.submit_allowed_routes {
            let route = allowed_route.trim();
            if route.is_empty() {
                continue;
            }
            if find_route_cap(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_max_slippage_bps is missing cap for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_MAX_SLIPPAGE_BPS format route:cap)",
                    route
                ));
            }
            if find_route_tip(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_tip_lamports is missing tip for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_TIP_LAMPORTS format route:tip)",
                    route
                ));
            }
            if find_route_cu_limit(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_compute_unit_limit is missing limit for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_LIMIT format route:limit)",
                    route
                ));
            }
            if find_route_cu_price(route).is_none() {
                return Err(anyhow!(
                    "execution.submit_route_compute_unit_price_micro_lamports is missing price for allowed route={} (check SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS format route:price)",
                    route
                ));
            }
        }
        let default_route_cap = find_route_cap(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_max_slippage_bps is missing cap for default route={}",
                default_route
            )
        })?;
        find_route_tip(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_tip_lamports is missing tip for default route={}",
                default_route
            )
        })?;
        let default_route_limit = find_route_cu_limit(default_route.as_str()).ok_or_else(|| {
            anyhow!(
                "execution.submit_route_compute_unit_limit is missing limit for default route={}",
                default_route
            )
        })?;
        if config.slippage_bps > default_route_cap {
            return Err(anyhow!(
                "execution.slippage_bps ({}) cannot exceed cap ({}) for default route {}",
                config.slippage_bps,
                default_route_cap,
                default_route
            ));
        }
        if config.pretrade_max_priority_fee_lamports > 0 {
            for allowed_route in &config.submit_allowed_routes {
                let route = allowed_route.trim();
                if route.is_empty() {
                    continue;
                }
                let route_price = find_route_cu_price(route).ok_or_else(|| {
                    anyhow!(
                        "execution.submit_route_compute_unit_price_micro_lamports is missing price for allowed route={}",
                        route
                    )
                })?;
                if route_price > config.pretrade_max_priority_fee_lamports {
                    return Err(anyhow!(
                        "execution.submit_route_compute_unit_price_micro_lamports route {} price ({}) cannot exceed execution.pretrade_max_priority_fee_lamports ({}) (unit: micro-lamports per CU for both fields)",
                        route,
                        route_price,
                        config.pretrade_max_priority_fee_lamports
                    ));
                }
            }
        }
        if default_route_limit < EXECUTION_ROUTE_DEFAULT_COMPUTE_UNIT_LIMIT_MIN {
            return Err(anyhow!(
                "execution.submit_route_compute_unit_limit default route {} limit ({}) is too low for reliable swaps; expected >= {}",
                default_route,
                default_route_limit,
                EXECUTION_ROUTE_DEFAULT_COMPUTE_UNIT_LIMIT_MIN
            ));
        }
        if config.submit_dynamic_cu_price_enabled && config.pretrade_max_priority_fee_lamports == 0
        {
            return Err(anyhow!(
                "execution.submit_dynamic_cu_price_enabled=true requires execution.pretrade_max_priority_fee_lamports > 0 to cap dynamic compute unit price"
            ));
        }
        if config.submit_dynamic_tip_lamports_enabled {
            if !config.submit_dynamic_cu_price_enabled {
                return Err(anyhow!(
                    "execution.submit_dynamic_tip_lamports_enabled=true requires execution.submit_dynamic_cu_price_enabled=true"
                ));
            }
            if config.submit_dynamic_tip_lamports_multiplier_bps == 0
                || config.submit_dynamic_tip_lamports_multiplier_bps > 100_000
            {
                return Err(anyhow!(
                    "execution.submit_dynamic_tip_lamports_multiplier_bps must be in 1..=100000 when dynamic tip policy is enabled"
                ));
            }
        }
    } else if config.submit_dynamic_cu_price_enabled {
        return Err(anyhow!(
            "execution.submit_dynamic_cu_price_enabled is only supported in execution.mode=adapter_submit_confirm"
        ));
    } else if config.submit_dynamic_tip_lamports_enabled {
        return Err(anyhow!(
            "execution.submit_dynamic_tip_lamports_enabled is only supported in execution.mode=adapter_submit_confirm"
        ));
    }

    Ok(())
}

fn is_production_env_profile(env: &str) -> bool {
    let env_norm = env.trim().to_ascii_lowercase();
    matches!(env_norm.as_str(), "prod" | "production")
        || env_norm.starts_with("prod-")
        || env_norm.starts_with("prod_")
        || env_norm.starts_with("production-")
        || env_norm.starts_with("production_")
}

fn is_valid_contract_version_token(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_'))
}

fn validate_unique_normalized_route_list(values: &[String], field_name: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            return Err(anyhow!(
                "{field_name} contains duplicate route after normalization: {}",
                normalized
            ));
        }
    }
    Ok(())
}

fn validate_unique_normalized_route_map_keys<T>(
    values: &BTreeMap<String, T>,
    field_name: &str,
) -> Result<()> {
    let mut seen = HashSet::new();
    for key in values.keys() {
        let normalized = key.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            return Err(anyhow!(
                "{field_name} contains duplicate route key after normalization: {}",
                normalized
            ));
        }
    }
    Ok(())
}

fn validate_adapter_endpoint_url(
    endpoint: &str,
    field_name: &str,
    strict_transport_policy: bool,
) -> Result<()> {
    if contains_placeholder_value(endpoint.trim()) {
        return Err(anyhow!(
            "{field_name} must not contain placeholder value REPLACE_ME"
        ));
    }
    if endpoint.chars().any(char::is_whitespace) {
        return Err(anyhow!("{field_name} must not contain whitespace"));
    }
    let endpoint_trimmed = endpoint.trim();
    let endpoint_norm = endpoint_trimmed.to_ascii_lowercase();
    let has_valid_scheme =
        endpoint_norm.starts_with("https://") || endpoint_norm.starts_with("http://");
    if !has_valid_scheme {
        return Err(anyhow!(
            "{field_name} must start with http:// or https:// (got: {})",
            endpoint
        ));
    }
    let parsed = Url::parse(endpoint_trimmed)
        .map_err(|error| anyhow!("{field_name} must be a valid http(s) URL: {error}"))?;
    let scheme = parsed.scheme();
    if !matches!(scheme, "http" | "https") {
        return Err(anyhow!(
            "{field_name} must use http:// or https:// scheme (got: {})",
            scheme
        ));
    }
    let Some(host) = parsed.host() else {
        return Err(anyhow!("{field_name} must include a host"));
    };
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(anyhow!(
            "{field_name} must not embed credentials in URL (use submit_adapter_auth_token / HMAC fields)"
        ));
    }
    if parsed.query().is_some() {
        return Err(anyhow!(
            "{field_name} must not include query parameters (pass auth/policy via headers)"
        ));
    }
    if parsed.fragment().is_some() {
        return Err(anyhow!("{field_name} must not include URL fragment"));
    }
    if strict_transport_policy && scheme == "http" && !is_loopback_host(&host) {
        return Err(anyhow!(
            "{field_name} must use https:// in production-like envs (http:// allowed only for loopback hosts)"
        ));
    }

    Ok(())
}

fn adapter_endpoint_identity(endpoint: &str) -> Result<String> {
    let parsed = Url::parse(endpoint.trim())
        .map_err(|error| anyhow!("invalid adapter endpoint: {error}"))?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("adapter endpoint missing host"))?
        .to_ascii_lowercase();
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("adapter endpoint missing known default port for scheme"))?;
    let path = if parsed.path().is_empty() {
        "/".to_string()
    } else {
        parsed.path().to_string()
    };
    Ok(format!("{scheme}://{host}:{port}{path}"))
}

fn is_loopback_host(host: &Host<&str>) -> bool {
    match host {
        Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(ip) => ip.is_loopback(),
        Host::Ipv6(ip) => ip.is_loopback(),
    }
}
