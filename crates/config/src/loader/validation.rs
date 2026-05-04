use anyhow::{anyhow, Result};

use crate::AppConfig;

pub(super) fn validate_loaded_config(config: &AppConfig) -> Result<()> {
    validate_shadow_universe_config(config)?;
    validate_shadow_quality_thresholds(config)?;
    validate_discovery_storage_mitigation_config(config)?;
    validate_recent_raw_journal_config(config)?;
    validate_runtime_restore_ops_config(config)?;
    validate_discovery_aggregate_activation_config(config)?;
    validate_risk_sol_boundary_config(config)?;
    validate_history_retention_config(config)?;
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
    if config.discovery.scoring_aggregates_enabled
        && !config.discovery.scoring_aggregates_write_enabled
    {
        return Err(anyhow!(
            "discovery.scoring_aggregates_enabled requires discovery.scoring_aggregates_write_enabled = true"
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

fn validate_discovery_storage_mitigation_config(config: &AppConfig) -> Result<()> {
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
