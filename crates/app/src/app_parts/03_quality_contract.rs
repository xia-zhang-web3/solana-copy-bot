fn select_role_helius_http_url(role_specific: &str, fallback: &str) -> Option<String> {
    let role_specific = role_specific.trim();
    if !role_specific.is_empty() && !contains_placeholder_value(role_specific) {
        return Some(role_specific.to_string());
    }

    let fallback = fallback.trim();
    if !fallback.is_empty() && !contains_placeholder_value(fallback) {
        return Some(fallback.to_string());
    }

    None
}

fn enforce_quality_gate_http_url(
    role: &str,
    env: &str,
    quality_gates_enabled: bool,
    endpoint: Option<String>,
) -> Result<Option<String>> {
    let env_norm = env.trim().to_ascii_lowercase();
    let enforce = quality_gates_enabled
        && (matches!(env_norm.as_str(), "paper" | "prod")
            || env_norm.starts_with("paper-")
            || env_norm.starts_with("prod-"));
    if enforce && endpoint.is_none() {
        return Err(anyhow!(
            "{role} requires a valid helius_http_url (role-specific or ingestion fallback) when quality gates are enabled in {env}"
        ));
    }
    Ok(endpoint)
}

fn validate_shadow_quality_gate_contract(config: &ShadowConfig, env: &str) -> Result<()> {
    if !config.quality_gates_enabled {
        return Ok(());
    }
    if !config.min_liquidity_sol.is_finite() || config.min_liquidity_sol < 0.0 {
        return Err(anyhow!(
            "shadow.min_liquidity_sol must be finite and >= 0 when quality gates are enabled in {}",
            env
        ));
    }
    if !config.min_volume_5m_sol.is_finite() || config.min_volume_5m_sol < 0.0 {
        return Err(anyhow!(
            "shadow.min_volume_5m_sol must be finite and >= 0 when quality gates are enabled in {}",
            env
        ));
    }
    let all_thresholds_zero = config.min_token_age_seconds == 0
        && config.min_holders == 0
        && config.min_liquidity_sol <= 0.0
        && config.min_volume_5m_sol <= 0.0
        && config.min_unique_traders_5m == 0;
    if all_thresholds_zero {
        return Err(anyhow!(
            "shadow.quality_gates_enabled=true but all quality thresholds are zero in {}; set at least one non-zero threshold",
            env
        ));
    }
    Ok(())
}
