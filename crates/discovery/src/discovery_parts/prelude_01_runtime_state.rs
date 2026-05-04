#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalletSnapshot {
    wallet_id: String,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    pnl_sol: f64,
    win_rate: f64,
    trades: u32,
    closed_trades: u32,
    hold_median_seconds: i64,
    score: f64,
    buy_total: u32,
    tradable_ratio: f64,
    rug_ratio: f64,
    eligible: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct RugMetrics {
    evaluated: u32,
    rugged: u32,
    unevaluated: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuyFactRugStatus {
    Healthy,
    Rugged,
    Unevaluated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Lot {
    qty: f64,
    cost_sol: f64,
    opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
struct BuyObservation {
    token: String,
    ts: DateTime<Utc>,
    tradable: bool,
    quality_resolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingBuyRugCheck {
    token: String,
    wallet_id: String,
    buy_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
struct FetchProgress {
    query_rows: usize,
    query_rows_last_page: usize,
    pages: usize,
    saturated: bool,
    page_budget_exhausted: bool,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone)]
enum PreparedCycleState {
    Cached {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        summary: DiscoverySummary,
        current_raw: Option<CachedCurrentRawTruthSample>,
    },
    Degraded {
        publish_due: bool,
        active_wallets: HashSet<String>,
        scoring_source: &'static str,
    },
    BootstrapDegraded {
        active_wallets: HashSet<String>,
        scoring_source: &'static str,
    },
    Unusable {
        publish_due: bool,
        scoring_source: &'static str,
    },
    PersistedRecompute {
        publish_due: bool,
        scoring_source: &'static str,
        empty_window_degraded_scoring_source: &'static str,
        empty_window_bootstrap_degraded_scoring_source: &'static str,
        empty_window_unusable_scoring_source: &'static str,
    },
    Recompute {
        publish_due: bool,
        followlist_activations_suppressed: bool,
        followlist_deactivations_suppressed: bool,
        metrics_persistence_suppressed: bool,
        swaps: VecDeque<SwapEvent>,
    },
}

#[derive(Debug, Clone, Default)]
struct InBandWalletFreshnessCaptureTelemetry {
    state: &'static str,
    reason: Option<String>,
    capture_id: Option<i64>,
    captured_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SolLegTrade {
    ts: DateTime<Utc>,
    wallet_id: String,
    sol_notional: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct TokenRollingState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    first_seen: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    wallets_seen: HashSet<String>,
    sol_trades_5m: VecDeque<SolLegTrade>,
    sol_volume_5m: f64,
    sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WalletAccumulator {
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    trades: u32,
    #[serde(default)]
    exact_active_day_count: Option<u32>,
    spent_sol: f64,
    realized_pnl_sol: f64,
    max_buy_notional_sol: f64,
    wins: u32,
    closed_trades: u32,
    hold_samples_sec: Vec<i64>,
    active_days: HashSet<NaiveDate>,
    realized_pnl_by_day: HashMap<NaiveDate, f64>,
    tx_per_minute: HashMap<i64, u32>,
    suspicious: bool,
    positions: HashMap<String, VecDeque<Lot>>,
    #[serde(default)]
    buy_mints: BTreeSet<String>,
    buy_total: u32,
    quality_resolved_buys: u32,
    tradable_buys: u32,
    #[serde(default)]
    publish_pending_quality_retry_buy_count: u32,
    rug_metrics: RugMetrics,
    buy_observations: Vec<BuyObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum CollectBuyMintsMode {
    #[default]
    FreshScan,
    ReconcileExpiredHead,
    ReconcileNewTail,
}

impl CollectBuyMintsMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::FreshScan => "fresh_scan",
            Self::ReconcileExpiredHead => "reconcile_expired_head",
            Self::ReconcileNewTail => "reconcile_new_tail",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum ReplayMode {
    #[default]
    LegacyFullWindow,
    WalletStatsThenSolLeg,
}

impl ReplayMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::LegacyFullWindow => "legacy_full_window",
            Self::WalletStatsThenSolLeg => "wallet_stats_then_sol_leg",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayWalletStatsDayCountSourceProgress {
    fast_path_pages_processed: usize,
    fallback_pages_processed: usize,
    fast_path_wallets_processed: usize,
    fallback_wallets_processed: usize,
}
