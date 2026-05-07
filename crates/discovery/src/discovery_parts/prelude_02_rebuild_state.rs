use super::*;

impl ReplayWalletStatsDayCountSourceProgress {
    pub(crate) fn observe_page(
        &mut self,
        source: Option<ObservedWalletActivityDayCountSource>,
        wallet_count: usize,
    ) {
        match source {
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays) => {
                self.fast_path_pages_processed = self.fast_path_pages_processed.saturating_add(1);
                self.fast_path_wallets_processed = self
                    .fast_path_wallets_processed
                    .saturating_add(wallet_count);
            }
            Some(ObservedWalletActivityDayCountSource::ObservedSwapsFallback) => {
                self.fallback_pages_processed = self.fallback_pages_processed.saturating_add(1);
                self.fallback_wallets_processed =
                    self.fallback_wallets_processed.saturating_add(wallet_count);
            }
            None => {}
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.fast_path_pages_processed = self
            .fast_path_pages_processed
            .saturating_add(other.fast_path_pages_processed);
        self.fallback_pages_processed = self
            .fallback_pages_processed
            .saturating_add(other.fallback_pages_processed);
        self.fast_path_wallets_processed = self
            .fast_path_wallets_processed
            .saturating_add(other.fast_path_wallets_processed);
        self.fallback_wallets_processed = self
            .fallback_wallets_processed
            .saturating_add(other.fallback_wallets_processed);
    }

    pub(crate) fn merged_with(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }

    pub(crate) fn total_pages_processed(self) -> usize {
        self.fast_path_pages_processed
            .saturating_add(self.fallback_pages_processed)
    }

    pub(crate) fn total_wallets_processed(self) -> usize {
        self.fast_path_wallets_processed
            .saturating_add(self.fallback_wallets_processed)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PersistedStreamRebuildPayload {
    pub(crate) unique_buy_mints: Vec<String>,
    #[serde(default)]
    pub(crate) discovery_critical_target_buy_mints: Vec<String>,
    #[serde(default)]
    pub(crate) buy_mint_counts: BTreeMap<String, u32>,
    #[serde(default)]
    pub(crate) collect_buy_mints_cursor_token: Option<String>,
    #[serde(default)]
    pub(crate) collect_buy_mints_prepass_complete: bool,
    #[serde(default)]
    pub(crate) collect_buy_mints_mode: CollectBuyMintsMode,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_expired_head_pending_mints: Vec<String>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    #[serde(default)]
    pub(crate) collect_buy_mints_reconcile_new_tail_pending_mints: Vec<String>,
    #[serde(default)]
    pub(crate) replay_mode: ReplayMode,
    #[serde(default)]
    pub(crate) replay_wallet_stats_complete: bool,
    #[serde(default)]
    pub(crate) replay_wallet_stats_rows_processed: usize,
    #[serde(default)]
    pub(crate) replay_wallet_stats_pages_processed: usize,
    #[serde(default)]
    pub(crate) replay_wallet_stats_wallet_cursor: Option<String>,
    #[serde(default)]
    pub(crate) replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    #[serde(default)]
    pub(crate) replay_wallet_stats_budget_floor_wallets: usize,
    #[serde(default)]
    pub(crate) replay_wallet_stats_last_partial_cycle_pages_processed: usize,
    #[serde(default)]
    pub(crate) replay_wallet_stats_last_partial_cycle_wallets_processed: usize,
    #[serde(default)]
    pub(crate) replay_wallet_stats_last_partial_cycle_elapsed_ms: u64,
    #[serde(default)]
    pub(crate) replay_wallet_stats_milestone_reached: bool,
    #[serde(default)]
    pub(crate) replay_sol_leg_reentry_pending: bool,
    #[serde(default)]
    pub(crate) replay_sol_leg_last_partial_cycle_pages_processed: usize,
    #[serde(default)]
    pub(crate) replay_sol_leg_last_partial_cycle_rows_processed: usize,
    #[serde(default)]
    pub(crate) replay_sol_leg_last_partial_cycle_elapsed_ms: u64,
    #[serde(default)]
    pub(crate) replay_sol_leg_budget_floor_pages: usize,
    #[serde(default)]
    pub(crate) replay_sol_leg_retained_contract_floor_pages: usize,
    #[serde(default)]
    pub(crate) replay_candidate_activity_backfill_required: bool,
    #[serde(default)]
    pub(crate) replay_candidate_activity_backfill_pending: bool,
    #[serde(default)]
    pub(crate) replay_candidate_activity_backfill_wallet_cursor: Option<String>,
    #[serde(default)]
    pub(crate) replay_exact_target_surface_wallet_cursor: Option<String>,
    #[serde(default)]
    pub(crate) replay_exact_target_surface_pre_row_blocked: bool,
    #[serde(default)]
    pub(crate) replay_exact_target_surface_staged_wallet_ids: Vec<String>,
    #[serde(default)]
    pub(crate) replay_exact_target_surface_staged_wallet_cursor_after: Option<String>,
    pub(crate) token_quality_cache: HashMap<String, quality_cache::TokenQualityResolution>,
    pub(crate) token_quality_progress: quality_cache::TokenQualityResolutionProgress,
    pub(crate) by_wallet: HashMap<String, WalletAccumulator>,
    pub(crate) token_states: HashMap<String, TokenRollingState>,
    pub(crate) token_recent_sol_trades: HashMap<String, VecDeque<SolLegTrade>>,
    pub(crate) pending_rug_checks: VecDeque<PendingBuyRugCheck>,
    pub(crate) token_pending_buy_starts: HashMap<String, VecDeque<DateTime<Utc>>>,
    pub(crate) completed_snapshots: Vec<WalletSnapshot>,
    #[serde(default)]
    pub(crate) publish_pending_requested_wallet_ids: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) publish_pending_quality_retry_mints: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedStreamRebuildState {
    pub(crate) phase: DiscoveryPersistedRebuildPhase,
    pub(crate) window_start: DateTime<Utc>,
    pub(crate) horizon_end: DateTime<Utc>,
    pub(crate) metrics_window_start: DateTime<Utc>,
    pub(crate) phase_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) prepass_rows_processed: usize,
    pub(crate) prepass_pages_processed: usize,
    pub(crate) replay_rows_processed: usize,
    pub(crate) replay_pages_processed: usize,
    pub(crate) chunks_completed: usize,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) payload: PersistedStreamRebuildPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistedStreamBudgetExhaustedReason {
    TimeBudget,
    PageBudget,
}

impl PersistedStreamBudgetExhaustedReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TimeBudget => "time_budget",
            Self::PageBudget => "page_budget",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PersistedStreamProgressTelemetry {
    pub(crate) phase: DiscoveryPersistedRebuildPhase,
    pub(crate) collect_buy_mints_mode: CollectBuyMintsMode,
    pub(crate) replay_mode: ReplayMode,
    pub(crate) replay_subphase: Option<&'static str>,
    pub(crate) window_start: DateTime<Utc>,
    pub(crate) horizon_end: DateTime<Utc>,
    pub(crate) metrics_window_start: DateTime<Utc>,
    pub(crate) phase_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) replay_wallet_stats_wallet_cursor: Option<String>,
    pub(crate) collect_buy_mints_cursor_token: Option<String>,
    pub(crate) collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    pub(crate) collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    pub(crate) collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    pub(crate) collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    pub(crate) collect_buy_mints_reconcile_expired_head_pending_mints: usize,
    pub(crate) collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    pub(crate) collect_buy_mints_reconcile_new_tail_pending_mints: usize,
    pub(crate) prepass_rows_processed: usize,
    pub(crate) prepass_pages_processed: usize,
    pub(crate) replay_wallet_stats_complete: bool,
    pub(crate) replay_wallet_stats_rows_processed: usize,
    pub(crate) replay_wallet_stats_pages_processed: usize,
    pub(crate) replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    pub(crate) replay_wallet_stats_budget_floor_wallets: usize,
    pub(crate) replay_wallet_stats_last_partial_cycle_pages_processed: usize,
    pub(crate) replay_wallet_stats_last_partial_cycle_wallets_processed: usize,
    pub(crate) replay_wallet_stats_last_partial_cycle_elapsed_ms: u64,
    pub(crate) replay_wallet_stats_publishable_horizon_remaining_ms: Option<u64>,
    pub(crate) replay_wallet_stats_milestone_reached: bool,
    pub(crate) replay_sol_leg_reentry_pending: bool,
    pub(crate) replay_sol_leg_last_partial_cycle_pages_processed: usize,
    pub(crate) replay_sol_leg_last_partial_cycle_rows_processed: usize,
    pub(crate) replay_sol_leg_last_partial_cycle_elapsed_ms: u64,
    pub(crate) replay_sol_leg_budget_floor_pages: usize,
    pub(crate) replay_sol_leg_publishable_horizon_remaining_ms: Option<u64>,
    pub(crate) replay_sol_leg_retained_contract_floor_pages: usize,
    pub(crate) replay_candidate_activity_backfill_required: bool,
    pub(crate) replay_candidate_activity_backfill_wallet_cursor: Option<String>,
    pub(crate) replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    pub(crate) replay_rows_processed: usize,
    pub(crate) replay_pages_processed: usize,
    pub(crate) chunks_completed: usize,
    pub(crate) cycle_rows_processed: usize,
    pub(crate) cycle_pages_processed: usize,
    pub(crate) cycle_replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    pub(crate) cycle_replay_wallet_stats_wallet_cursor_before: Option<String>,
    pub(crate) cycle_replay_wallet_stats_wallet_cursor_after: Option<String>,
    pub(crate) cycle_unique_buy_mints_discovered: usize,
    pub(crate) observed_swaps_loaded: usize,
    pub(crate) unique_buy_mints: usize,
    pub(crate) quality_next_mint_index: usize,
    pub(crate) quality_rpc_attempted: usize,
    pub(crate) quality_rpc_spent_ms: u64,
    pub(crate) wallets_buffered: usize,
    pub(crate) publish_pending_requested_wallet_count: usize,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) cycle_elapsed_ms: u64,
    pub(crate) total_elapsed_ms: u64,
    pub(crate) partial: bool,
    pub(crate) completed: bool,
    pub(crate) budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
}
