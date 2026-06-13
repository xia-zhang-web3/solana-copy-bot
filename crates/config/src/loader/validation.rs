use anyhow::{anyhow, Result};

use crate::risk_validation::validate_shadow_risk_float_gates;
use crate::AppConfig;

pub(super) fn validate_loaded_config(config: &AppConfig) -> Result<()> {
    validate_live_ingestion_source_config(config)?;
    validate_shadow_universe_config(config)?;
    validate_shadow_quality_thresholds(config)?;
    validate_discovery_v2_float_gates(config)?;
    validate_shadow_risk_float_gates(config)?;
    validate_discovery_storage_mitigation_config(config)?;
    validate_recent_raw_journal_config(config)?;
    validate_runtime_restore_ops_config(config)?;
    validate_discovery_aggregate_activation_config(config)?;
    validate_risk_sol_boundary_config(config)?;
    validate_history_retention_config(config)?;
    Ok(())
}

fn validate_live_ingestion_source_config(config: &AppConfig) -> Result<()> {
    let env = config.system.env.trim().to_ascii_lowercase();
    let prod_like = matches!(env.as_str(), "prod" | "production" | "live" | "prod-live")
        || env.starts_with("prod-");
    if prod_like && config.ingestion.source == "mock" {
        return Err(anyhow!(
            "ingestion.source=mock is not allowed in production env {}; use yellowstone_grpc",
            config.system.env
        ));
    }
    Ok(())
}

fn validate_shadow_universe_config(config: &AppConfig) -> Result<()> {
    let follow_top_n = u64::from(config.discovery.follow_top_n);
    let min_active = config.risk.shadow_universe_min_active_follow_wallets;
    if follow_top_n < min_active {
        return Err(anyhow!(
            "discovery.follow_top_n ({follow_top_n}) must be >= risk.shadow_universe_min_active_follow_wallets ({min_active})"
        ));
    }
    Ok(())
}

fn validate_discovery_aggregate_activation_config(config: &AppConfig) -> Result<()> {
    if config.discovery.scoring_aggregates_write_enabled
        || config.discovery.scoring_aggregates_enabled
    {
        return Err(anyhow!(
            "legacy discovery aggregate scoring is not allowed in active runtime config"
        ));
    }
    Ok(())
}

fn validate_shadow_quality_thresholds(config: &AppConfig) -> Result<()> {
    let min_holders = config.shadow.min_holders;
    if (1..5).contains(&min_holders) {
        return Err(anyhow!(
            "shadow.min_holders ({min_holders}) must be either 0 (disable holder gate) or >= 5"
        ));
    }
    Ok(())
}

fn validate_discovery_v2_float_gates(config: &AppConfig) -> Result<()> {
    validate_finite_non_negative(
        "discovery.min_leader_notional_sol",
        config.discovery.min_leader_notional_sol,
    )?;
    validate_finite_non_negative("discovery.min_score", config.discovery.min_score)?;
    validate_finite_ratio(
        "discovery.maturity_score_bonus",
        config.discovery.maturity_score_bonus,
    )?;
    validate_finite_ratio(
        "discovery.min_tradable_ratio",
        config.discovery.min_tradable_ratio,
    )?;
    validate_finite_non_negative(
        "discovery.min_live_sol_balance",
        config.discovery.min_live_sol_balance,
    )?;
    validate_finite_non_negative(
        "discovery.min_live_portfolio_value_sol",
        config.discovery.min_live_portfolio_value_sol,
    )?;
    validate_finite_ratio("discovery.max_rug_ratio", config.discovery.max_rug_ratio)?;
    validate_finite_non_negative(
        "discovery.thin_market_min_volume_sol",
        config.discovery.thin_market_min_volume_sol,
    )?;
    if config.discovery.executable_wallet_filter_window_hours == 0 {
        return Err(anyhow!(
            "discovery.executable_wallet_filter_window_hours ({}) must be >= 1",
            config.discovery.executable_wallet_filter_window_hours
        ));
    }
    if config.discovery.executable_wallet_filter_min_samples == 0 {
        return Err(anyhow!(
            "discovery.executable_wallet_filter_min_samples ({}) must be >= 1",
            config.discovery.executable_wallet_filter_min_samples
        ));
    }
    validate_finite(
        "discovery.executable_wallet_filter_max_pnl_sol",
        config.discovery.executable_wallet_filter_max_pnl_sol,
    )?;
    validate_finite_ratio(
        "discovery.executable_wallet_filter_max_flip_rate",
        config.discovery.executable_wallet_filter_max_flip_rate,
    )?;
    if config.discovery.rug_wallet_filter_window_hours == 0 {
        return Err(anyhow!(
            "discovery.rug_wallet_filter_window_hours ({}) must be >= 1",
            config.discovery.rug_wallet_filter_window_hours
        ));
    }
    if config.discovery.rug_wallet_filter_min_closed_trades == 0 {
        return Err(anyhow!(
            "discovery.rug_wallet_filter_min_closed_trades ({}) must be >= 1",
            config.discovery.rug_wallet_filter_min_closed_trades
        ));
    }
    validate_finite_ratio(
        "discovery.rug_wallet_filter_max_stale_terminal_rate",
        config.discovery.rug_wallet_filter_max_stale_terminal_rate,
    )?;
    validate_finite(
        "discovery.rug_wallet_filter_max_stale_terminal_pnl_sol",
        config
            .discovery
            .rug_wallet_filter_max_stale_terminal_pnl_sol,
    )?;
    validate_finite_non_negative(
        "shadow.min_leader_notional_sol",
        config.shadow.min_leader_notional_sol,
    )?;
    validate_finite_non_negative("shadow.min_liquidity_sol", config.shadow.min_liquidity_sol)?;
    validate_finite_non_negative("shadow.min_volume_5m_sol", config.shadow.min_volume_5m_sol)?;
    Ok(())
}

fn validate_finite(label: &str, value: f64) -> Result<()> {
    if !value.is_finite() {
        return Err(anyhow!("{label} must be finite, got {value}"));
    }
    Ok(())
}

fn validate_finite_non_negative(label: &str, value: f64) -> Result<()> {
    validate_finite(label, value)?;
    if value < 0.0 {
        return Err(anyhow!("{label} must be >= 0, got {value}"));
    }
    Ok(())
}

fn validate_finite_ratio(label: &str, value: f64) -> Result<()> {
    validate_finite_non_negative(label, value)?;
    if value > 1.0 {
        return Err(anyhow!("{label} must be <= 1, got {value}"));
    }
    Ok(())
}

fn validate_discovery_storage_mitigation_config(config: &AppConfig) -> Result<()> {
    let scoring_window_minutes =
        u64::from(config.discovery.scoring_window_days.max(1)).saturating_mul(24 * 60);
    if config.discovery.status_scan_window_minutes > scoring_window_minutes {
        return Err(anyhow!(
            "discovery.status_scan_window_minutes ({}) must be <= discovery.scoring_window_days window minutes ({})",
            config.discovery.status_scan_window_minutes,
            scoring_window_minutes
        ));
    }
    if config.discovery.min_active_days > config.discovery.scoring_window_days {
        return Err(anyhow!(
            "discovery.min_active_days ({}) must be <= discovery.scoring_window_days ({})",
            config.discovery.min_active_days,
            config.discovery.scoring_window_days
        ));
    }
    if config.discovery.publish_min_candidate_wallets > config.discovery.follow_top_n {
        return Err(anyhow!(
            "discovery.publish_min_candidate_wallets ({}) must be <= discovery.follow_top_n ({})",
            config.discovery.publish_min_candidate_wallets,
            config.discovery.follow_top_n
        ));
    }
    if config.discovery.maturity_min_active_days > 0 {
        if config.discovery.maturity_window_days == 0 {
            return Err(anyhow!(
                "discovery.maturity_window_days must be >= 1 when discovery.maturity_min_active_days ({}) is enabled",
                config.discovery.maturity_min_active_days
            ));
        }
        if config.discovery.maturity_min_active_days > config.discovery.maturity_window_days {
            return Err(anyhow!(
                "discovery.maturity_min_active_days ({}) must be <= discovery.maturity_window_days ({})",
                config.discovery.maturity_min_active_days,
                config.discovery.maturity_window_days
            ));
        }
    }
    if config.discovery.max_fetch_swaps_per_cycle > config.discovery.max_window_swaps_in_memory {
        return Err(anyhow!(
            "discovery.max_fetch_swaps_per_cycle ({}) must be <= discovery.max_window_swaps_in_memory ({})",
            config.discovery.max_fetch_swaps_per_cycle,
            config.discovery.max_window_swaps_in_memory
        ));
    }
    if config.discovery.max_fetch_pages_per_cycle == 0 {
        return Err(anyhow!(
            "discovery.max_fetch_pages_per_cycle ({}) must be >= 1",
            config.discovery.max_fetch_pages_per_cycle
        ));
    }
    if config.discovery.fetch_refresh_seconds == 0 {
        return Err(anyhow!(
            "discovery.fetch_refresh_seconds ({}) must be >= 1",
            config.discovery.fetch_refresh_seconds
        ));
    }
    if config.discovery.refresh_seconds < config.discovery.fetch_refresh_seconds {
        return Err(anyhow!(
            "discovery.refresh_seconds ({}) must be >= discovery.fetch_refresh_seconds ({})",
            config.discovery.refresh_seconds,
            config.discovery.fetch_refresh_seconds
        ));
    }
    if config.discovery.fetch_time_budget_ms == 0 {
        return Err(anyhow!(
            "discovery.fetch_time_budget_ms ({}) must be >= 1",
            config.discovery.fetch_time_budget_ms
        ));
    }
    if config.discovery.live_portfolio_gate_enabled {
        if config.discovery.live_portfolio_max_wallets
            < config.discovery.follow_top_n.max(1) as usize
        {
            return Err(anyhow!(
                "discovery.live_portfolio_max_wallets ({}) must be >= discovery.follow_top_n ({})",
                config.discovery.live_portfolio_max_wallets,
                config.discovery.follow_top_n
            ));
        }
        if config.discovery.live_portfolio_request_timeout_ms == 0 {
            return Err(anyhow!(
                "discovery.live_portfolio_request_timeout_ms ({}) must be >= 1",
                config.discovery.live_portfolio_request_timeout_ms
            ));
        }
        if config.discovery.live_portfolio_max_token_accounts == 0 {
            return Err(anyhow!(
                "discovery.live_portfolio_max_token_accounts ({}) must be >= 1",
                config.discovery.live_portfolio_max_token_accounts
            ));
        }
        if config.discovery.min_live_sol_balance <= 0.0
            && config.discovery.min_live_portfolio_value_sol <= 0.0
        {
            return Err(anyhow!(
                "discovery live portfolio gate requires min_live_sol_balance or min_live_portfolio_value_sol > 0"
            ));
        }
    }
    Ok(())
}

fn validate_recent_raw_journal_config(config: &AppConfig) -> Result<()> {
    let journal = &config.recent_raw_journal;
    if journal.path.trim().is_empty() {
        return Err(anyhow!("recent_raw_journal.path cannot be empty"));
    }
    if journal.retention_safety_buffer_days == 0 {
        return Err(anyhow!(
            "recent_raw_journal.retention_safety_buffer_days ({}) must be >= 1",
            journal.retention_safety_buffer_days
        ));
    }
    if journal.writer_queue_capacity_batches == 0 {
        return Err(anyhow!(
            "recent_raw_journal.writer_queue_capacity_batches ({}) must be >= 1",
            journal.writer_queue_capacity_batches
        ));
    }
    if journal.replay_batch_size == 0 {
        return Err(anyhow!(
            "recent_raw_journal.replay_batch_size ({}) must be >= 1",
            journal.replay_batch_size
        ));
    }
    Ok(())
}

fn validate_runtime_restore_ops_config(config: &AppConfig) -> Result<()> {
    let ops = &config.runtime_restore_ops;
    if ops.artifact_dir.trim().is_empty() {
        return Err(anyhow!("runtime_restore_ops.artifact_dir cannot be empty"));
    }
    if ops.artifact_retention == 0 {
        return Err(anyhow!(
            "runtime_restore_ops.artifact_retention ({}) must be >= 1",
            ops.artifact_retention
        ));
    }
    if ops.artifact_cadence_minutes == 0 {
        return Err(anyhow!(
            "runtime_restore_ops.artifact_cadence_minutes ({}) must be >= 1",
            ops.artifact_cadence_minutes
        ));
    }
    if ops.journal_snapshot_dir.trim().is_empty() {
        return Err(anyhow!(
            "runtime_restore_ops.journal_snapshot_dir cannot be empty"
        ));
    }
    if ops.journal_snapshot_retention == 0 {
        return Err(anyhow!(
            "runtime_restore_ops.journal_snapshot_retention ({}) must be >= 1",
            ops.journal_snapshot_retention
        ));
    }
    if ops.journal_snapshot_cadence_minutes == 0 {
        return Err(anyhow!(
            "runtime_restore_ops.journal_snapshot_cadence_minutes ({}) must be >= 1",
            ops.journal_snapshot_cadence_minutes
        ));
    }
    if ops.drill_workspace_dir.trim().is_empty() {
        return Err(anyhow!(
            "runtime_restore_ops.drill_workspace_dir cannot be empty"
        ));
    }
    let freshness_gate_minutes = config
        .discovery
        .metric_snapshot_interval_seconds
        .max(60)
        .div_ceil(60);
    if ops.artifact_cadence_minutes >= freshness_gate_minutes {
        return Err(anyhow!(
            "runtime_restore_ops.artifact_cadence_minutes ({}) must be < freshness gate bucket minutes ({freshness_gate_minutes})",
            ops.artifact_cadence_minutes
        ));
    }
    if ops.journal_snapshot_cadence_minutes > freshness_gate_minutes {
        return Err(anyhow!(
            "runtime_restore_ops.journal_snapshot_cadence_minutes ({}) must be <= freshness gate bucket minutes ({freshness_gate_minutes})",
            ops.journal_snapshot_cadence_minutes
        ));
    }
    Ok(())
}

fn validate_risk_sol_boundary_config(config: &AppConfig) -> Result<()> {
    validate_sol_boundary("risk.max_position_sol", config.risk.max_position_sol, false)?;
    validate_sol_boundary(
        "risk.max_total_exposure_sol",
        config.risk.max_total_exposure_sol,
        false,
    )?;
    validate_sol_boundary(
        "risk.max_exposure_per_token_sol",
        config.risk.max_exposure_per_token_sol,
        false,
    )?;
    Ok(())
}

fn validate_sol_boundary(label: &str, value: f64, allow_zero: bool) -> Result<()> {
    if !value.is_finite() {
        return Err(anyhow!("{label} must be finite, got {value}"));
    }
    if value < 0.0 || (!allow_zero && value <= 0.0) {
        return Err(anyhow!(
            "{label} must be {} 0, got {value}",
            if allow_zero { ">=" } else { ">" }
        ));
    }

    let lamports = value * 1_000_000_000.0;
    if !lamports.is_finite() || lamports > u64::MAX as f64 {
        return Err(anyhow!("{label} overflows lamports, got {value}"));
    }
    if !allow_zero && value > 0.0 && lamports < 1.0 {
        return Err(anyhow!(
            "{label} must be representable as at least 1 lamport, got {value}"
        ));
    }
    Ok(())
}

fn validate_history_retention_config(config: &AppConfig) -> Result<()> {
    let retention = &config.history_retention;
    if !retention.enabled {
        return Ok(());
    }

    let protected_history_days = retention.protected_history_days;
    if protected_history_days == 0 {
        return Err(anyhow!(
            "history_retention.protected_history_days ({protected_history_days}) must be >= 1"
        ));
    }

    let sweep_seconds = retention.sweep_seconds;
    if sweep_seconds == 0 {
        return Err(anyhow!(
            "history_retention.sweep_seconds ({sweep_seconds}) must be >= 1"
        ));
    }

    for (name, value) in [
        ("risk_events_days", retention.risk_events_days),
        ("copy_signals_days", retention.copy_signals_days),
        ("orders_days", retention.orders_days),
        ("fills_days", retention.fills_days),
        (
            "shadow_closed_trades_days",
            retention.shadow_closed_trades_days,
        ),
    ] {
        if value == 0 {
            return Err(anyhow!(
                "history_retention.{name} ({value}) must be >= 1 when history_retention.enabled=true"
            ));
        }
    }

    let orders_days = retention.orders_days;
    let fills_days = retention.fills_days;
    if fills_days != orders_days {
        return Err(anyhow!(
            "history_retention.fills_days ({fills_days}) must equal history_retention.orders_days ({orders_days}) to preserve order/fill query parity"
        ));
    }

    let copy_signals_days = retention.copy_signals_days;
    if copy_signals_days < orders_days {
        return Err(anyhow!(
            "history_retention.copy_signals_days ({copy_signals_days}) must be >= history_retention.orders_days ({orders_days}) for FK-safe execution history retention"
        ));
    }

    let effective_shadow_closed_trades_days = retention
        .shadow_closed_trades_days
        .max(protected_history_days);
    let required_shadow_closed_trade_days =
        ((config.risk.shadow_rug_loss_window_minutes.max(24 * 60) + 24 * 60 - 1) / (24 * 60))
            as u32;
    if effective_shadow_closed_trades_days < required_shadow_closed_trade_days {
        return Err(anyhow!(
            "effective shadow closed trade retention ({effective_shadow_closed_trades_days} days) must be >= {} days to preserve runtime risk windows",
            required_shadow_closed_trade_days
        ));
    }

    Ok(())
}
