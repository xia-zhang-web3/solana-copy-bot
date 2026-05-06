pub const STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES: i64 = 30;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL: f64 = 0.05;
pub const STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES: usize = 3;
pub const STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES: usize = 60;

pub const SHADOW_CLOSE_CONTEXT_MARKET: &str = "market";
pub const SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE: &str = "stale_terminal_zero_price";
pub const SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE: &str = "recovery_terminal_zero_price";
pub const SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY: &str = "quarantined_legacy";

pub const SHADOW_RISK_CONTEXT_MARKET: &str = "market";
pub const SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY: &str = "quarantined_legacy";
