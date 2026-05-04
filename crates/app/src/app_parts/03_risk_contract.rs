fn validate_execution_risk_contract(config: &RiskConfig) -> Result<()> {
    if !config.max_position_sol.is_finite() || config.max_position_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_position_sol must be finite and > 0, got {}",
            config.max_position_sol
        ));
    }
    if !config.max_total_exposure_sol.is_finite() || config.max_total_exposure_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_total_exposure_sol must be finite and > 0, got {}",
            config.max_total_exposure_sol
        ));
    }
    if !config.max_exposure_per_token_sol.is_finite() || config.max_exposure_per_token_sol <= 0.0 {
        return Err(anyhow!(
            "risk.max_exposure_per_token_sol must be finite and > 0, got {}",
            config.max_exposure_per_token_sol
        ));
    }
    if config.max_exposure_per_token_sol > config.max_total_exposure_sol {
        return Err(anyhow!(
            "risk.max_exposure_per_token_sol ({}) cannot exceed risk.max_total_exposure_sol ({})",
            config.max_exposure_per_token_sol,
            config.max_total_exposure_sol
        ));
    }
    if config.max_concurrent_positions == 0 {
        return Err(anyhow!(
            "risk.max_concurrent_positions must be >= 1, got {}",
            config.max_concurrent_positions
        ));
    }
    if config.max_copy_delay_sec == 0 {
        return Err(anyhow!(
            "risk.max_copy_delay_sec must be >= 1, got {}",
            config.max_copy_delay_sec
        ));
    }
    if config.max_hold_hours == 0 && config.shadow_stale_close_terminal_zero_price_hours > 0 {
        return Err(anyhow!(
            "risk.shadow_stale_close_terminal_zero_price_hours ({}) requires risk.max_hold_hours >= 1",
            config.shadow_stale_close_terminal_zero_price_hours
        ));
    }
    if config.max_hold_hours == 0 && config.shadow_stale_close_recovery_zero_price_enabled {
        return Err(anyhow!(
            "risk.shadow_stale_close_recovery_zero_price_enabled=true requires risk.max_hold_hours >= 1"
        ));
    }
    if config.shadow_stale_close_recovery_zero_price_enabled
        && config.shadow_stale_close_terminal_zero_price_hours <= config.max_hold_hours
    {
        return Err(anyhow!(
            "risk.shadow_stale_close_recovery_zero_price_enabled=true requires risk.shadow_stale_close_terminal_zero_price_hours ({}) > risk.max_hold_hours ({})",
            config.shadow_stale_close_terminal_zero_price_hours,
            config.max_hold_hours
        ));
    }
    if config.shadow_stale_close_terminal_zero_price_hours > 0 {
        let min_terminal_zero_price_hours = config.max_hold_hours.saturating_mul(2);
        if config.shadow_stale_close_terminal_zero_price_hours < min_terminal_zero_price_hours {
            return Err(anyhow!(
                "risk.shadow_stale_close_terminal_zero_price_hours ({}) must be 0 (disabled) or >= 2 * risk.max_hold_hours ({})",
                config.shadow_stale_close_terminal_zero_price_hours,
                min_terminal_zero_price_hours
            ));
        }
    }
    if !config.daily_loss_limit_pct.is_finite()
        || config.daily_loss_limit_pct < 0.0
        || config.daily_loss_limit_pct > 100.0
    {
        return Err(anyhow!(
            "risk.daily_loss_limit_pct must be finite and in [0, 100], got {}",
            config.daily_loss_limit_pct
        ));
    }
    if !config.max_drawdown_pct.is_finite()
        || config.max_drawdown_pct < 0.0
        || config.max_drawdown_pct > 100.0
    {
        return Err(anyhow!(
            "risk.max_drawdown_pct must be finite and in [0, 100], got {}",
            config.max_drawdown_pct
        ));
    }

    if !config.shadow_soft_exposure_cap_sol.is_finite()
        || config.shadow_soft_exposure_cap_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_cap_sol must be finite and > 0, got {}",
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if !config.shadow_soft_exposure_resume_below_sol.is_finite()
        || config.shadow_soft_exposure_resume_below_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_resume_below_sol must be finite and > 0, got {}",
            config.shadow_soft_exposure_resume_below_sol
        ));
    }
    if config.shadow_soft_exposure_resume_below_sol > config.shadow_soft_exposure_cap_sol {
        return Err(anyhow!(
            "risk.shadow_soft_exposure_resume_below_sol ({}) must be <= risk.shadow_soft_exposure_cap_sol ({})",
            config.shadow_soft_exposure_resume_below_sol,
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if !config.shadow_hard_exposure_cap_sol.is_finite()
        || config.shadow_hard_exposure_cap_sol <= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_hard_exposure_cap_sol must be finite and > 0, got {}",
            config.shadow_hard_exposure_cap_sol
        ));
    }
    if config.shadow_hard_exposure_cap_sol < config.shadow_soft_exposure_cap_sol {
        return Err(anyhow!(
            "risk.shadow_hard_exposure_cap_sol ({}) must be >= risk.shadow_soft_exposure_cap_sol ({})",
            config.shadow_hard_exposure_cap_sol,
            config.shadow_soft_exposure_cap_sol
        ));
    }
    if config.shadow_killswitch_enabled && config.shadow_soft_pause_minutes == 0 {
        return Err(anyhow!(
            "risk.shadow_soft_pause_minutes must be >= 1 when risk.shadow_killswitch_enabled=true"
        ));
    }

    if !config.shadow_drawdown_1h_stop_sol.is_finite()
        || !config.shadow_drawdown_6h_stop_sol.is_finite()
        || !config.shadow_drawdown_24h_stop_sol.is_finite()
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_*_stop_sol values must be finite (1h={}, 6h={}, 24h={})",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if config.shadow_drawdown_1h_stop_sol > 0.0
        || config.shadow_drawdown_6h_stop_sol > 0.0
        || config.shadow_drawdown_24h_stop_sol > 0.0
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_*_stop_sol values must be <= 0 (1h={}, 6h={}, 24h={})",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if !(config.shadow_drawdown_1h_stop_sol >= config.shadow_drawdown_6h_stop_sol
        && config.shadow_drawdown_6h_stop_sol >= config.shadow_drawdown_24h_stop_sol)
    {
        return Err(anyhow!(
            "risk shadow drawdown ordering is invalid: expected 1h >= 6h >= 24h, got 1h={}, 6h={}, 24h={}",
            config.shadow_drawdown_1h_stop_sol,
            config.shadow_drawdown_6h_stop_sol,
            config.shadow_drawdown_24h_stop_sol
        ));
    }
    if config.shadow_killswitch_enabled
        && (config.shadow_drawdown_1h_pause_minutes == 0
            || config.shadow_drawdown_6h_pause_minutes == 0)
    {
        return Err(anyhow!(
            "risk.shadow_drawdown_1h_pause_minutes and risk.shadow_drawdown_6h_pause_minutes must be >= 1 when risk.shadow_killswitch_enabled=true"
        ));
    }

    if !config.shadow_rug_loss_return_threshold.is_finite()
        || config.shadow_rug_loss_return_threshold >= 0.0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_return_threshold must be finite and < 0, got {}",
            config.shadow_rug_loss_return_threshold
        ));
    }
    if config.shadow_rug_loss_window_minutes == 0
        || config.shadow_rug_loss_count_threshold == 0
        || config.shadow_rug_loss_rate_sample_size == 0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_window_minutes/count_threshold/rate_sample_size must be >= 1"
        ));
    }
    if !config.shadow_rug_loss_rate_threshold.is_finite()
        || config.shadow_rug_loss_rate_threshold <= 0.0
        || config.shadow_rug_loss_rate_threshold > 1.0
    {
        return Err(anyhow!(
            "risk.shadow_rug_loss_rate_threshold must be finite and in (0, 1], got {}",
            config.shadow_rug_loss_rate_threshold
        ));
    }

    if config.shadow_infra_window_minutes == 0 || config.shadow_infra_lag_breach_minutes == 0 {
        return Err(anyhow!(
            "risk.shadow_infra_window_minutes and risk.shadow_infra_lag_breach_minutes must be >= 1"
        ));
    }
    if config.shadow_infra_lag_p95_threshold_ms == 0 {
        return Err(anyhow!(
            "risk.shadow_infra_lag_p95_threshold_ms must be >= 1"
        ));
    }
    if !config.shadow_infra_replaced_ratio_threshold.is_finite()
        || config.shadow_infra_replaced_ratio_threshold <= 0.0
        || config.shadow_infra_replaced_ratio_threshold > 1.0
    {
        return Err(anyhow!(
            "risk.shadow_infra_replaced_ratio_threshold must be finite and in (0, 1], got {}",
            config.shadow_infra_replaced_ratio_threshold
        ));
    }

    if config.shadow_universe_min_active_follow_wallets == 0
        || config.shadow_universe_min_eligible_wallets == 0
        || config.shadow_universe_breach_cycles == 0
    {
        return Err(anyhow!(
            "risk.shadow_universe_min_active_follow_wallets/min_eligible_wallets/breach_cycles must be >= 1"
        ));
    }
    Ok(())
}
