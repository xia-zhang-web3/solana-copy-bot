#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuyRiskBlockReason {
    HardStop,
    ExposureCap,
    TimedPause,
    Infra,
    Universe,
    FailClosed,
    OperatorEmergencyStop,
}

impl BuyRiskBlockReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::HardStop => "risk_hard_stop",
            Self::ExposureCap => "risk_exposure_hard_cap",
            Self::TimedPause => "risk_timed_pause",
            Self::Infra => "risk_infra_stop",
            Self::Universe => "risk_universe_stop",
            Self::FailClosed => "risk_fail_closed",
            Self::OperatorEmergencyStop => "operator_emergency_stop",
        }
    }
}

#[derive(Debug)]
enum BuyRiskDecision {
    Allow,
    Blocked {
        reason: BuyRiskBlockReason,
        detail: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InfraBlockKey {
    LagP95,
    NoIngestionProgress,
    ParserStall,
    ReplacedRatio,
    Rpc429,
    Rpc5xx,
}

#[derive(Debug, Clone)]
struct InfraBlockSignal {
    key: InfraBlockKey,
    reason: String,
}

#[derive(Debug, Default)]
struct ShadowRiskGuard {
    config: RiskConfig,
    ingestion_source: String,
    hard_stop_reason: Option<String>,
    hard_stop_clear_healthy_streak: u64,
    last_db_refresh_error: Option<String>,
    exposure_hard_blocked: bool,
    exposure_hard_detail: Option<String>,
    pause_until: Option<DateTime<Utc>>,
    pause_reason: Option<String>,
    soft_exposure_pause_latched: bool,
    soft_exposure_pause_until: Option<DateTime<Utc>>,
    soft_exposure_pause_reason: Option<String>,
    universe_breach_streak: u64,
    universe_blocked: bool,
    infra_samples: VecDeque<IngestionRuntimeSnapshot>,
    lag_breach_since: Option<DateTime<Utc>>,
    infra_block_key: Option<InfraBlockKey>,
    infra_candidate_key: Option<InfraBlockKey>,
    infra_candidate_streak: u64,
    infra_healthy_streak: u64,
    infra_block_reason: Option<String>,
    infra_last_event_at: Option<DateTime<Utc>>,
    last_db_refresh_at: Option<DateTime<Utc>>,
    last_fail_closed_log_at: Option<DateTime<Utc>>,
}
