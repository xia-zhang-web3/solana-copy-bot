use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WalletSnapshot {
    pub(crate) wallet_id: String,
    pub(crate) first_seen: DateTime<Utc>,
    pub(crate) last_seen: DateTime<Utc>,
    pub(crate) pnl_sol: f64,
    pub(crate) win_rate: f64,
    pub(crate) trades: u32,
    pub(crate) closed_trades: u32,
    pub(crate) hold_median_seconds: i64,
    pub(crate) score: f64,
    pub(crate) buy_total: u32,
    pub(crate) tradable_ratio: f64,
    pub(crate) rug_ratio: f64,
    pub(crate) eligible: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub(crate) struct RugMetrics {
    pub(crate) evaluated: u32,
    pub(crate) rugged: u32,
    pub(crate) unevaluated: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuyFactRugStatus {
    Healthy,
    Rugged,
    Unevaluated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Lot {
    pub(crate) qty: f64,
    pub(crate) cost_sol: f64,
    pub(crate) opened_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct BuyObservation {
    pub(crate) token: String,
    pub(crate) ts: DateTime<Utc>,
    pub(crate) tradable: bool,
    pub(crate) quality_resolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PendingBuyRugCheck {
    pub(crate) token: String,
    pub(crate) wallet_id: String,
    pub(crate) buy_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FetchProgress {
    pub(crate) query_rows: usize,
    pub(crate) query_rows_last_page: usize,
    pub(crate) pages: usize,
    pub(crate) saturated: bool,
    pub(crate) page_budget_exhausted: bool,
    pub(crate) time_budget_exhausted: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PreparedCycleState {
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
pub(crate) struct InBandWalletFreshnessCaptureTelemetry {
    pub(crate) state: &'static str,
    pub(crate) reason: Option<String>,
    pub(crate) capture_id: Option<i64>,
    pub(crate) captured_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SolLegTrade {
    pub(crate) ts: DateTime<Utc>,
    pub(crate) wallet_id: String,
    pub(crate) sol_notional: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TokenRollingState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) first_seen: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub(crate) wallets_seen: HashSet<String>,
    pub(crate) sol_trades_5m: VecDeque<SolLegTrade>,
    pub(crate) sol_volume_5m: f64,
    pub(crate) sol_traders_5m: HashMap<String, u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct WalletAccumulator {
    pub(crate) first_seen: Option<DateTime<Utc>>,
    pub(crate) last_seen: Option<DateTime<Utc>>,
    pub(crate) trades: u32,
    #[serde(default)]
    pub(crate) exact_active_day_count: Option<u32>,
    pub(crate) spent_sol: f64,
    pub(crate) realized_pnl_sol: f64,
    pub(crate) max_buy_notional_sol: f64,
    pub(crate) wins: u32,
    pub(crate) closed_trades: u32,
    pub(crate) hold_samples_sec: Vec<i64>,
    pub(crate) active_days: HashSet<NaiveDate>,
    pub(crate) realized_pnl_by_day: HashMap<NaiveDate, f64>,
    pub(crate) tx_per_minute: HashMap<i64, u32>,
    pub(crate) suspicious: bool,
    pub(crate) positions: HashMap<String, VecDeque<Lot>>,
    #[serde(default)]
    pub(crate) buy_mints: BTreeSet<String>,
    pub(crate) buy_total: u32,
    pub(crate) quality_resolved_buys: u32,
    pub(crate) tradable_buys: u32,
    #[serde(default)]
    pub(crate) publish_pending_quality_retry_buy_count: u32,
    pub(crate) rug_metrics: RugMetrics,
    pub(crate) buy_observations: Vec<BuyObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum CollectBuyMintsMode {
    #[default]
    FreshScan,
    ReconcileExpiredHead,
    ReconcileNewTail,
}

impl CollectBuyMintsMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::FreshScan => "fresh_scan",
            Self::ReconcileExpiredHead => "reconcile_expired_head",
            Self::ReconcileNewTail => "reconcile_new_tail",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub(crate) enum ReplayMode {
    #[default]
    LegacyCompleteReplay,
    WalletStatsThenSolLeg,
}

impl<'de> Deserialize<'de> for ReplayMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = <String as Deserialize>::deserialize(deserializer)?;
        match raw.as_str() {
            "LegacyCompleteReplay" => Ok(Self::LegacyCompleteReplay),
            "WalletStatsThenSolLeg" => Ok(Self::WalletStatsThenSolLeg),
            value if value == ["Legacy", "Full", "Window"].concat() => {
                Ok(Self::LegacyCompleteReplay)
            }
            _ => Err(serde::de::Error::unknown_variant(
                raw.as_str(),
                &["LegacyCompleteReplay", "WalletStatsThenSolLeg"],
            )),
        }
    }
}

impl ReplayMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LegacyCompleteReplay => "legacy_complete_replay",
            Self::WalletStatsThenSolLeg => "wallet_stats_then_sol_leg",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReplayWalletStatsDayCountSourceProgress {
    pub(crate) fast_path_pages_processed: usize,
    pub(crate) fallback_pages_processed: usize,
    pub(crate) fast_path_wallets_processed: usize,
    pub(crate) fallback_wallets_processed: usize,
}
