use chrono::{DateTime, Utc};
use copybot_config::RiskConfig;
use copybot_ingestion::IngestionRuntimeSnapshot;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuyRiskBlockReason {
    HardStop,
    ExposureCap,
    TimedPause,
    Infra,
    Universe,
    TokenCooldown,
    WalletCooldown,
    WalletTokenCooldown,
    FailClosed,
    OperatorEmergencyStop,
}

impl BuyRiskBlockReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HardStop => "risk_hard_stop",
            Self::ExposureCap => "risk_exposure_hard_cap",
            Self::TimedPause => "risk_timed_pause",
            Self::Infra => "risk_infra_stop",
            Self::Universe => "risk_universe_stop",
            Self::TokenCooldown => "risk_token_loss_cooldown",
            Self::WalletCooldown => "risk_wallet_loss_cooldown",
            Self::WalletTokenCooldown => "risk_wallet_token_fast_loss_cooldown",
            Self::FailClosed => "risk_fail_closed",
            Self::OperatorEmergencyStop => "operator_emergency_stop",
        }
    }
}

#[derive(Debug)]
pub(crate) enum BuyRiskDecision {
    Allow,
    Blocked {
        reason: BuyRiskBlockReason,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InfraBlockKey {
    LagP95,
    NoIngestionProgress,
    ParserStall,
    ReplacedRatio,
    Rpc429,
    Rpc5xx,
}

#[derive(Debug, Clone)]
pub(crate) struct InfraBlockSignal {
    pub(crate) key: InfraBlockKey,
    pub(crate) reason: String,
}

#[derive(Debug, Default)]
pub(crate) struct ShadowRiskGuard {
    pub(crate) config: RiskConfig,
    pub(crate) ingestion_source: String,
    pub(crate) hard_stop_reason: Option<String>,
    pub(crate) hard_stop_clear_healthy_streak: u64,
    pub(crate) last_db_refresh_error: Option<String>,
    pub(crate) exposure_hard_blocked: bool,
    pub(crate) exposure_hard_detail: Option<String>,
    pub(crate) pause_until: Option<DateTime<Utc>>,
    pub(crate) pause_reason: Option<String>,
    pub(crate) soft_exposure_pause_latched: bool,
    pub(crate) soft_exposure_pause_until: Option<DateTime<Utc>>,
    pub(crate) soft_exposure_pause_reason: Option<String>,
    pub(crate) universe_breach_streak: u64,
    pub(crate) universe_blocked: bool,
    pub(crate) infra_samples: VecDeque<IngestionRuntimeSnapshot>,
    pub(crate) lag_breach_since: Option<DateTime<Utc>>,
    pub(crate) infra_block_key: Option<InfraBlockKey>,
    pub(crate) infra_candidate_key: Option<InfraBlockKey>,
    pub(crate) infra_candidate_streak: u64,
    pub(crate) infra_healthy_streak: u64,
    pub(crate) infra_block_reason: Option<String>,
    pub(crate) infra_last_event_at: Option<DateTime<Utc>>,
    pub(crate) last_db_refresh_at: Option<DateTime<Utc>>,
    pub(crate) last_fail_closed_log_at: Option<DateTime<Utc>>,
}
