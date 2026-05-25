use chrono::{DateTime, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{Lamports, TokenQuantity};
use std::collections::{HashMap, HashSet};

pub(crate) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(crate) const EPS: f64 = 1e-12;
pub(crate) const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
pub(crate) const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
pub(crate) const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;

pub(crate) fn sol_to_lamports_floor(sol: f64) -> Option<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return None;
    }
    let scaled = sol * 1_000_000_000.0;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return None;
    }
    Some(Lamports::new(scaled.floor() as u64))
}

pub(crate) fn sol_to_lamports_ceil(sol: f64) -> Option<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return None;
    }
    let scaled = sol * 1_000_000_000.0;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return None;
    }
    Some(Lamports::new(scaled.ceil() as u64))
}

pub(crate) fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / 1_000_000_000.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScaledExactShadowQty {
    Exact(TokenQuantity),
    Approximate,
    InvalidZeroRaw,
}

pub(crate) fn scaled_exact_shadow_qty(
    exact_token_qty: Option<TokenQuantity>,
    exact_leader_notional_lamports: Option<Lamports>,
    copy_notional_lamports: Option<Lamports>,
    copy_notional_sol: f64,
) -> ScaledExactShadowQty {
    let Some(exact_token_qty) = exact_token_qty else {
        return ScaledExactShadowQty::Approximate;
    };
    let Some(leader_notional) = exact_leader_notional_lamports else {
        return ScaledExactShadowQty::Approximate;
    };
    if leader_notional == Lamports::ZERO {
        return ScaledExactShadowQty::Approximate;
    }
    let Some(copy_notional) =
        copy_notional_lamports.or_else(|| sol_to_lamports_floor(copy_notional_sol))
    else {
        return ScaledExactShadowQty::Approximate;
    };
    if copy_notional > leader_notional {
        return ScaledExactShadowQty::Approximate;
    }
    let Some(scaled_raw) = u128::from(exact_token_qty.raw())
        .checked_mul(u128::from(copy_notional.as_u64()))
        .and_then(|value| value.checked_div(u128::from(leader_notional.as_u64())))
        .and_then(|value| u64::try_from(value).ok())
    else {
        return ScaledExactShadowQty::Approximate;
    };
    if scaled_raw == 0 && copy_notional != Lamports::ZERO {
        return ScaledExactShadowQty::InvalidZeroRaw;
    }
    ScaledExactShadowQty::Exact(TokenQuantity::new(scaled_raw, exact_token_qty.decimals()))
}

#[derive(Debug, Clone)]
pub struct ShadowService {
    pub(crate) config: ShadowConfig,
    pub(crate) copy_notional_lamports: Option<Lamports>,
    pub(crate) min_leader_notional_lamports: Option<Lamports>,
    pub(crate) helius_http_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ShadowSignalResult {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub latency_ms: i64,
    pub closed_qty: f64,
    pub realized_pnl_sol: f64,
    pub has_open_lots_after_signal: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShadowDropReason {
    Disabled,
    NotFollowed,
    NotSolLeg,
    BelowNotional,
    LagExceeded,
    TooNew,
    LowHolders,
    LowLiquidity,
    LowVolume,
    ThinMarket,
    RecentSellCooldown,
    InvalidSizing,
    DuplicateSignal,
    UnsupportedSide,
}

impl ShadowDropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NotFollowed => "not_followed",
            Self::NotSolLeg => "not_sol_leg",
            Self::BelowNotional => "below_notional",
            Self::LagExceeded => "lag_exceeded",
            Self::TooNew => "too_new",
            Self::LowHolders => "low_holders",
            Self::LowLiquidity => "low_liquidity",
            Self::LowVolume => "low_volume",
            Self::ThinMarket => "thin_market",
            Self::RecentSellCooldown => "recent_sell_cooldown",
            Self::InvalidSizing => "invalid_sizing",
            Self::DuplicateSignal => "duplicate_signal",
            Self::UnsupportedSide => "unsupported_side",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ShadowProcessOutcome {
    Recorded(ShadowSignalResult),
    Dropped(ShadowDropReason),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowSnapshot {
    pub closed_trades_24h: u64,
    pub realized_pnl_sol_24h: f64,
    pub open_lots: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FollowSnapshot {
    pub active: HashSet<String>,
    pub promoted_at: HashMap<String, DateTime<Utc>>,
    pub demoted_at: HashMap<String, DateTime<Utc>>,
}

impl FollowSnapshot {
    pub fn from_active_wallets(active: HashSet<String>) -> Self {
        Self {
            active,
            promoted_at: HashMap::new(),
            demoted_at: HashMap::new(),
        }
    }

    pub fn is_active(&self, wallet_id: &str) -> bool {
        self.active.contains(wallet_id)
    }

    pub fn is_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> bool {
        let promoted_at = self.promoted_at.get(wallet_id).cloned();
        let demoted_at = self.demoted_at.get(wallet_id).cloned();
        match (promoted_at, demoted_at) {
            // Promotion happened after the latest demotion: active from promotion onward.
            (Some(promoted), Some(demoted)) if promoted >= demoted => ts >= promoted,
            // Demotion happened after the latest promotion: active only in [promotion, demotion).
            (Some(promoted), Some(demoted)) => ts >= promoted && ts < demoted,
            (Some(promoted), None) => ts >= promoted,
            (None, Some(demoted)) => ts < demoted,
            (None, None) => self.is_active(wallet_id),
        }
    }
}
