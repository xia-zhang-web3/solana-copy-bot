use anyhow::Result;
use std::env;

use crate::env_parsing::{parse_env_bool, parse_env_number, parse_env_string_list};
use crate::AppConfig;

pub(super) fn apply_risk_env_overrides(config: &mut AppConfig) -> Result<()> {
    if let Some(max_position_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_POSITION_SOL", "f64")?
    {
        config.risk.max_position_sol = max_position_sol;
    }
    if let Some(max_total_exposure_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_TOTAL_EXPOSURE_SOL", "f64")?
    {
        config.risk.max_total_exposure_sol = max_total_exposure_sol;
    }
    if let Some(max_exposure_per_token_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_EXPOSURE_PER_TOKEN_SOL", "f64")?
    {
        config.risk.max_exposure_per_token_sol = max_exposure_per_token_sol;
    }
    if let Some(max_concurrent_positions) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_RISK_MAX_CONCURRENT_POSITIONS", "u32")?
    {
        config.risk.max_concurrent_positions = max_concurrent_positions;
    }
    if let Some(execution_buy_cooldown_seconds) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_EXECUTION_BUY_COOLDOWN_SECONDS", "u64")?
    {
        config.risk.execution_buy_cooldown_seconds = execution_buy_cooldown_seconds;
    }
    if let Some(daily_loss_limit_pct) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_DAILY_LOSS_LIMIT_PCT", "f64")?
    {
        config.risk.daily_loss_limit_pct = daily_loss_limit_pct;
    }
    if let Some(max_drawdown_pct) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_MAX_DRAWDOWN_PCT", "f64")?
    {
        config.risk.max_drawdown_pct = max_drawdown_pct;
    }
    if let Some(max_hold_hours) =
        parse_env_number::<u32>("SOLANA_COPY_BOT_RISK_MAX_HOLD_HOURS", "u32")?
    {
        config.risk.max_hold_hours = max_hold_hours;
    }
    if let Some(shadow_stale_close_terminal_zero_price_hours) = parse_env_number::<u32>(
        "SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_TERMINAL_ZERO_PRICE_HOURS",
        "u32",
    )? {
        config.risk.shadow_stale_close_terminal_zero_price_hours =
            shadow_stale_close_terminal_zero_price_hours;
    }
    if let Some(shadow_stale_close_recovery_zero_price_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_RISK_SHADOW_STALE_CLOSE_RECOVERY_ZERO_PRICE_ENABLED")?
    {
        config.risk.shadow_stale_close_recovery_zero_price_enabled =
            shadow_stale_close_recovery_zero_price_enabled;
    }
    if let Some(max_copy_delay_sec) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_MAX_COPY_DELAY_SEC", "u64")?
    {
        config.risk.max_copy_delay_sec = max_copy_delay_sec;
    }
    if let Some(shadow_killswitch_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_RISK_SHADOW_KILLSWITCH_ENABLED")?
    {
        config.risk.shadow_killswitch_enabled = shadow_killswitch_enabled;
    }
    if let Some(shadow_soft_exposure_cap_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_CAP_SOL", "f64")?
    {
        config.risk.shadow_soft_exposure_cap_sol = shadow_soft_exposure_cap_sol;
    }
    if let Some(shadow_soft_exposure_resume_below_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_SOFT_EXPOSURE_RESUME_BELOW_SOL",
        "f64",
    )? {
        config.risk.shadow_soft_exposure_resume_below_sol = shadow_soft_exposure_resume_below_sol;
    }
    if let Some(shadow_soft_pause_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_SOFT_PAUSE_MINUTES", "u64")?
    {
        config.risk.shadow_soft_pause_minutes = shadow_soft_pause_minutes;
    }
    if let Some(shadow_hard_exposure_cap_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_HARD_EXPOSURE_CAP_SOL", "f64")?
    {
        config.risk.shadow_hard_exposure_cap_sol = shadow_hard_exposure_cap_sol;
    }
    if let Some(shadow_drawdown_1h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_1h_stop_sol = shadow_drawdown_1h_stop_sol;
    }
    if let Some(shadow_drawdown_1h_pause_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_1H_PAUSE_MINUTES",
        "u64",
    )? {
        config.risk.shadow_drawdown_1h_pause_minutes = shadow_drawdown_1h_pause_minutes;
    }
    if let Some(shadow_drawdown_6h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_6h_stop_sol = shadow_drawdown_6h_stop_sol;
    }
    if let Some(shadow_drawdown_6h_pause_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_6H_PAUSE_MINUTES",
        "u64",
    )? {
        config.risk.shadow_drawdown_6h_pause_minutes = shadow_drawdown_6h_pause_minutes;
    }
    if let Some(shadow_drawdown_24h_stop_sol) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_DRAWDOWN_24H_STOP_SOL", "f64")?
    {
        config.risk.shadow_drawdown_24h_stop_sol = shadow_drawdown_24h_stop_sol;
    }
    if let Some(shadow_rug_loss_return_threshold) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RETURN_THRESHOLD",
        "f64",
    )? {
        config.risk.shadow_rug_loss_return_threshold = shadow_rug_loss_return_threshold;
    }
    if let Some(shadow_rug_loss_window_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_WINDOW_MINUTES", "u64")?
    {
        config.risk.shadow_rug_loss_window_minutes = shadow_rug_loss_window_minutes;
    }
    if let Some(shadow_rug_loss_count_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_COUNT_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_rug_loss_count_threshold = shadow_rug_loss_count_threshold;
    }
    if let Some(shadow_rug_loss_rate_sample_size) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_SAMPLE_SIZE",
        "u64",
    )? {
        config.risk.shadow_rug_loss_rate_sample_size = shadow_rug_loss_rate_sample_size;
    }
    if let Some(shadow_rug_loss_rate_threshold) =
        parse_env_number::<f64>("SOLANA_COPY_BOT_RISK_SHADOW_RUG_LOSS_RATE_THRESHOLD", "f64")?
    {
        config.risk.shadow_rug_loss_rate_threshold = shadow_rug_loss_rate_threshold;
    }
    if let Some(shadow_token_loss_cooldown_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_ENABLED")?
    {
        config.risk.shadow_token_loss_cooldown_enabled = shadow_token_loss_cooldown_enabled;
    }
    if let Some(shadow_token_loss_cooldown_window_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_WINDOW_MINUTES",
        "u64",
    )? {
        config.risk.shadow_token_loss_cooldown_window_minutes =
            shadow_token_loss_cooldown_window_minutes;
    }
    if let Some(shadow_token_loss_cooldown_count_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_COUNT_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_token_loss_cooldown_count_threshold =
            shadow_token_loss_cooldown_count_threshold;
    }
    if let Some(shadow_token_loss_cooldown_return_threshold) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_RETURN_THRESHOLD",
        "f64",
    )? {
        config.risk.shadow_token_loss_cooldown_return_threshold =
            shadow_token_loss_cooldown_return_threshold;
    }
    if let Some(shadow_token_loss_cooldown_catastrophe_min_entry_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_CATASTROPHE_MIN_ENTRY_SOL",
        "f64",
    )? {
        config
            .risk
            .shadow_token_loss_cooldown_catastrophe_min_entry_sol =
            shadow_token_loss_cooldown_catastrophe_min_entry_sol;
    }
    if let Some(shadow_token_loss_cooldown_catastrophe_max_roi) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_TOKEN_LOSS_COOLDOWN_CATASTROPHE_MAX_ROI",
        "f64",
    )? {
        config.risk.shadow_token_loss_cooldown_catastrophe_max_roi =
            shadow_token_loss_cooldown_catastrophe_max_roi;
    }
    if let Some(shadow_wallet_loss_cooldown_enabled) =
        parse_env_bool("SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_ENABLED")?
    {
        config.risk.shadow_wallet_loss_cooldown_enabled = shadow_wallet_loss_cooldown_enabled;
    }
    if let Some(shadow_wallet_loss_cooldown_window_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_WINDOW_MINUTES",
        "u64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_window_minutes =
            shadow_wallet_loss_cooldown_window_minutes;
    }
    if let Some(shadow_wallet_loss_cooldown_min_closed_trades) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_MIN_CLOSED_TRADES",
        "u64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_min_closed_trades =
            shadow_wallet_loss_cooldown_min_closed_trades;
    }
    if let Some(shadow_wallet_loss_cooldown_min_entry_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_MIN_ENTRY_SOL",
        "f64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_min_entry_sol =
            shadow_wallet_loss_cooldown_min_entry_sol;
    }
    if let Some(shadow_wallet_loss_cooldown_max_pnl_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_MAX_PNL_SOL",
        "f64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_max_pnl_sol =
            shadow_wallet_loss_cooldown_max_pnl_sol;
    }
    if let Some(shadow_wallet_loss_cooldown_max_roi) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_MAX_ROI",
        "f64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_max_roi = shadow_wallet_loss_cooldown_max_roi;
    }
    if let Some(shadow_wallet_loss_cooldown_catastrophe_min_closed_trades) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_CATASTROPHE_MIN_CLOSED_TRADES",
        "u64",
    )? {
        config
            .risk
            .shadow_wallet_loss_cooldown_catastrophe_min_closed_trades =
            shadow_wallet_loss_cooldown_catastrophe_min_closed_trades;
    }
    if let Some(shadow_wallet_loss_cooldown_catastrophe_min_entry_sol) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_CATASTROPHE_MIN_ENTRY_SOL",
        "f64",
    )? {
        config
            .risk
            .shadow_wallet_loss_cooldown_catastrophe_min_entry_sol =
            shadow_wallet_loss_cooldown_catastrophe_min_entry_sol;
    }
    if let Some(shadow_wallet_loss_cooldown_catastrophe_max_roi) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_WALLET_LOSS_COOLDOWN_CATASTROPHE_MAX_ROI",
        "f64",
    )? {
        config.risk.shadow_wallet_loss_cooldown_catastrophe_max_roi =
            shadow_wallet_loss_cooldown_catastrophe_max_roi;
    }
    if let Some(shadow_infra_window_minutes) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_INFRA_WINDOW_MINUTES", "u64")?
    {
        config.risk.shadow_infra_window_minutes = shadow_infra_window_minutes;
    }
    if let Some(shadow_infra_lag_p95_threshold_ms) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_P95_THRESHOLD_MS",
        "u64",
    )? {
        config.risk.shadow_infra_lag_p95_threshold_ms = shadow_infra_lag_p95_threshold_ms;
    }
    if let Some(shadow_infra_lag_breach_minutes) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_LAG_BREACH_MINUTES",
        "u64",
    )? {
        config.risk.shadow_infra_lag_breach_minutes = shadow_infra_lag_breach_minutes;
    }
    if let Some(shadow_infra_replaced_ratio_threshold) = parse_env_number::<f64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_REPLACED_RATIO_THRESHOLD",
        "f64",
    )? {
        config.risk.shadow_infra_replaced_ratio_threshold = shadow_infra_replaced_ratio_threshold;
    }
    if let Some(shadow_infra_rpc429_delta_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC429_DELTA_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_infra_rpc429_delta_threshold = shadow_infra_rpc429_delta_threshold;
    }
    if let Some(shadow_infra_rpc5xx_delta_threshold) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_INFRA_RPC5XX_DELTA_THRESHOLD",
        "u64",
    )? {
        config.risk.shadow_infra_rpc5xx_delta_threshold = shadow_infra_rpc5xx_delta_threshold;
    }
    if let Some(shadow_universe_min_active_follow_wallets) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ACTIVE_FOLLOW_WALLETS",
        "u64",
    )? {
        config.risk.shadow_universe_min_active_follow_wallets =
            shadow_universe_min_active_follow_wallets;
    }
    if let Some(shadow_universe_min_eligible_wallets) = parse_env_number::<u64>(
        "SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_MIN_ELIGIBLE_WALLETS",
        "u64",
    )? {
        config.risk.shadow_universe_min_eligible_wallets = shadow_universe_min_eligible_wallets;
    }
    if let Some(shadow_universe_breach_cycles) =
        parse_env_number::<u64>("SOLANA_COPY_BOT_RISK_SHADOW_UNIVERSE_BREACH_CYCLES", "u64")?
    {
        config.risk.shadow_universe_breach_cycles = shadow_universe_breach_cycles;
    }
    if let Ok(program_ids_csv) = env::var("SOLANA_COPY_BOT_PROGRAM_IDS") {
        config.ingestion.subscribe_program_ids =
            parse_env_string_list(&program_ids_csv, "SOLANA_COPY_BOT_PROGRAM_IDS")?;
    }

    Ok(())
}
