use anyhow::{anyhow, Result};

use crate::AppConfig;

pub(crate) fn validate_shadow_risk_float_gates(config: &AppConfig) -> Result<()> {
    for (label, value) in [
        (
            "risk.shadow_drawdown_1h_stop_sol",
            config.risk.shadow_drawdown_1h_stop_sol,
        ),
        (
            "risk.shadow_drawdown_6h_stop_sol",
            config.risk.shadow_drawdown_6h_stop_sol,
        ),
        (
            "risk.shadow_drawdown_24h_stop_sol",
            config.risk.shadow_drawdown_24h_stop_sol,
        ),
        (
            "risk.shadow_rug_loss_return_threshold",
            config.risk.shadow_rug_loss_return_threshold,
        ),
        (
            "risk.shadow_token_loss_cooldown_return_threshold",
            config.risk.shadow_token_loss_cooldown_return_threshold,
        ),
        (
            "risk.shadow_token_loss_cooldown_catastrophe_max_roi",
            config.risk.shadow_token_loss_cooldown_catastrophe_max_roi,
        ),
        (
            "risk.shadow_wallet_loss_cooldown_max_pnl_sol",
            config.risk.shadow_wallet_loss_cooldown_max_pnl_sol,
        ),
        (
            "risk.shadow_wallet_loss_cooldown_max_roi",
            config.risk.shadow_wallet_loss_cooldown_max_roi,
        ),
        (
            "risk.shadow_wallet_loss_cooldown_catastrophe_max_roi",
            config.risk.shadow_wallet_loss_cooldown_catastrophe_max_roi,
        ),
    ] {
        validate_finite(label, value)?;
    }
    validate_finite_ratio(
        "risk.shadow_rug_loss_rate_threshold",
        config.risk.shadow_rug_loss_rate_threshold,
    )?;
    validate_finite_non_negative(
        "risk.shadow_token_loss_cooldown_catastrophe_min_entry_sol",
        config
            .risk
            .shadow_token_loss_cooldown_catastrophe_min_entry_sol,
    )?;
    validate_finite_non_negative(
        "risk.shadow_wallet_loss_cooldown_min_entry_sol",
        config.risk.shadow_wallet_loss_cooldown_min_entry_sol,
    )?;
    validate_finite_non_negative(
        "risk.shadow_wallet_loss_cooldown_catastrophe_min_entry_sol",
        config
            .risk
            .shadow_wallet_loss_cooldown_catastrophe_min_entry_sol,
    )?;
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
