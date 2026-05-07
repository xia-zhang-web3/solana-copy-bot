use super::*;

pub const DEFAULT_RECENT_CYCLES: usize = 3;
pub const DEFAULT_HISTORY_CAPTURE_LIMIT: usize = 5;
pub const DEFAULT_HISTORY_RECENT_HORIZON_MULTIPLIER: u64 = 2;

#[cfg(test)]
pub(super) static CURRENT_RAW_TRUTH_SAMPLE_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_current_raw_truth_sample_call_count_for_tests() {
    CURRENT_RAW_TRUTH_SAMPLE_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn current_raw_truth_sample_call_count_for_tests() -> usize {
    CURRENT_RAW_TRUTH_SAMPLE_CALLS.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletFreshnessVerdict {
    FreshCurrent,
    DriftingButAcceptable,
    StalePublicationTruth,
    InsufficientRawTruth,
    FailClosedNoPublicationTruth,
}

impl WalletFreshnessVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FreshCurrent => "fresh_current",
            Self::DriftingButAcceptable => "drifting_but_acceptable",
            Self::StalePublicationTruth => "stale_publication_truth",
            Self::InsufficientRawTruth => "insufficient_raw_truth",
            Self::FailClosedNoPublicationTruth => "fail_closed_no_publication_truth",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletUniverseComparison {
    pub left_count: usize,
    pub right_count: usize,
    pub overlap_count: usize,
    pub exact_match: bool,
    pub only_left: Vec<String>,
    pub only_right: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRawTruthStatus {
    pub sufficient: bool,
    pub reason: String,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_count: usize,
    pub short_retention_configured: bool,
    pub covered_since: Option<DateTime<Utc>>,
    pub covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub covered_through_lag_seconds: Option<u64>,
    pub tail_fresh_within_runtime_lag: bool,
    pub runtime_freshness_lag_seconds: u64,
    pub total_observed_swaps_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRawCycleSample {
    pub sample_now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PrecomputedWalletFreshnessCurrentRawTruth {
    pub window_start: DateTime<Utc>,
    pub observed_swaps_loaded: usize,
    pub eligible_wallet_count: usize,
    pub top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessRotationSignal {
    pub signal_available: bool,
    pub reason: Option<String>,
    pub cycles_requested: usize,
    pub cycles_completed: usize,
    pub sample_interval_seconds: u64,
    pub overlap_with_previous_cycle: Option<usize>,
    pub entered_since_previous_cycle: Vec<String>,
    pub left_since_previous_cycle: Vec<String>,
    pub stable_wallets_across_cycles: Vec<String>,
    pub unique_wallet_count_across_cycles: usize,
    pub samples: Vec<WalletFreshnessRawCycleSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessAuditReport {
    pub now: DateTime<Utc>,
    pub window_start: DateTime<Utc>,
    pub verdict: WalletFreshnessVerdict,
    pub reason: String,
    pub follow_top_n: usize,
    pub publication_truth_available: bool,
    pub publication_runtime_mode: Option<String>,
    pub publication_recent_under_gate: bool,
    pub latest_publication_ts: Option<DateTime<Utc>>,
    pub publication_age_seconds: Option<u64>,
    pub latest_publication_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Vec<String>,
    pub active_follow_wallet_ids: Vec<String>,
    pub current_raw_top_wallet_ids: Vec<String>,
    pub published_vs_current_raw: WalletUniverseComparison,
    pub active_follow_vs_current_raw: WalletUniverseComparison,
    pub active_follow_vs_published: WalletUniverseComparison,
    pub raw_truth: WalletFreshnessRawTruthStatus,
    pub rotation: WalletFreshnessRotationSignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletShadowSignalVerdict {
    NoSelectedWallets,
    NoRecentSelectedRawActivity,
    RecentSelectedRawActivityWithoutShadowSignals,
    ShadowSignalsPresentButConcentrated,
    ShadowSignalsPresentAndDistributed,
}

impl WalletShadowSignalVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoSelectedWallets => "no_selected_wallets",
            Self::NoRecentSelectedRawActivity => "no_recent_selected_raw_activity",
            Self::RecentSelectedRawActivityWithoutShadowSignals => {
                "recent_selected_raw_activity_without_shadow_signals"
            }
            Self::ShadowSignalsPresentButConcentrated => "shadow_signals_present_but_concentrated",
            Self::ShadowSignalsPresentAndDistributed => "shadow_signals_present_and_distributed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessShadowSignalEvidence {
    pub recent_window_start: DateTime<Utc>,
    pub recent_window_end: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_lookback_seconds: Option<u64>,
    pub selected_wallet_ids: Vec<String>,
    pub selected_wallet_count: usize,
    pub selected_wallets_with_recent_raw_activity: usize,
    pub selected_wallets_with_recent_shadow_signal: usize,
    pub recent_raw_swap_count: usize,
    pub recent_shadow_signal_count: usize,
    pub recent_raw_activity_wallet_ids: Vec<String>,
    pub recent_shadow_signal_wallet_ids: Vec<String>,
    pub recent_raw_activity_by_wallet: Vec<WalletRecentActivityCountRow>,
    pub recent_shadow_signal_by_wallet: Vec<WalletRecentActivityCountRow>,
    pub raw_activity_top_wallet_share: Option<f64>,
    pub shadow_signal_top_wallet_share: Option<f64>,
    pub raw_activity_broadly_distributed: bool,
    pub shadow_signal_broadly_distributed: bool,
    pub verdict: WalletShadowSignalVerdict,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessCaptureSnapshot {
    pub capture_id: Option<i64>,
    pub captured_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_age_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub within_recent_horizon: Option<bool>,
    pub recent_cycles: usize,
    pub audit: WalletFreshnessAuditReport,
    pub shadow_signal: WalletFreshnessShadowSignalEvidence,
}

#[derive(Debug, Clone)]
pub struct WalletFreshnessCaptureComputation {
    pub snapshot: WalletFreshnessCaptureSnapshot,
    pub raw_truth_build_duration_ms: u64,
    pub shadow_signal_duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletFreshnessHistoryVerdict {
    ValidatedCurrent,
    PartiallyValidatedButLowRotation,
    PublicationDrifting,
    InsufficientEvidence,
    RawTruthInsufficient,
}

impl WalletFreshnessHistoryVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ValidatedCurrent => "validated_current",
            Self::PartiallyValidatedButLowRotation => "partially_validated_but_low_rotation",
            Self::PublicationDrifting => "publication_drifting",
            Self::InsufficientEvidence => "insufficient_evidence",
            Self::RawTruthInsufficient => "raw_truth_insufficient",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletFreshnessHistoryReport {
    pub generated_at: DateTime<Utc>,
    pub captures_requested: usize,
    pub captures_loaded: usize,
    pub captures_considered: usize,
    pub captures_within_recent_horizon: usize,
    pub recent_horizon_seconds: u64,
    pub latest_capture_age_seconds: Option<u64>,
    pub stale_captures_excluded_from_verdict: bool,
    pub stale_captures_excluded_count: usize,
    pub verdict: WalletFreshnessHistoryVerdict,
    pub reason: String,
    pub fresh_capture_count: usize,
    pub drifting_capture_count: usize,
    pub stale_capture_count: usize,
    pub insufficient_raw_capture_count: usize,
    pub no_publication_truth_capture_count: usize,
    pub exact_published_current_match_count: usize,
    pub exact_active_current_match_count: usize,
    pub active_follow_change_count: usize,
    pub current_raw_change_count: usize,
    pub rotation_evidence_capture_count: usize,
    pub shadow_signal_present_capture_count: usize,
    pub broad_shadow_signal_capture_count: usize,
    pub captures: Vec<WalletFreshnessCaptureSnapshot>,
}

#[derive(Debug, Clone)]
pub(super) struct RawTruthSample {
    pub(super) window_start: DateTime<Utc>,
    pub(super) observed_swaps_loaded: usize,
    pub(super) eligible_wallet_count: usize,
    pub(super) top_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct RawTruthCyclePoint {
    pub(super) sample_now: DateTime<Utc>,
    pub(super) sample: RawTruthSample,
}
