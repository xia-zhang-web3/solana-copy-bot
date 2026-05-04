impl ReplayWalletStatsDayCountSourceProgress {
    fn observe_page(
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

    fn merge(&mut self, other: Self) {
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

    fn merged_with(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }

    fn total_pages_processed(self) -> usize {
        self.fast_path_pages_processed
            .saturating_add(self.fallback_pages_processed)
    }

    fn total_wallets_processed(self) -> usize {
        self.fast_path_wallets_processed
            .saturating_add(self.fallback_wallets_processed)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedStreamRebuildPayload {
    unique_buy_mints: Vec<String>,
    #[serde(default)]
    discovery_critical_target_buy_mints: Vec<String>,
    #[serde(default)]
    buy_mint_counts: BTreeMap<String, u32>,
    #[serde(default)]
    collect_buy_mints_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_prepass_complete: bool,
    #[serde(default)]
    collect_buy_mints_mode: CollectBuyMintsMode,
    #[serde(default)]
    collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    #[serde(default)]
    collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_expired_head_pending_mints: Vec<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    #[serde(default)]
    collect_buy_mints_reconcile_new_tail_pending_mints: Vec<String>,
    #[serde(default)]
    replay_mode: ReplayMode,
    #[serde(default)]
    replay_wallet_stats_complete: bool,
    #[serde(default)]
    replay_wallet_stats_rows_processed: usize,
    #[serde(default)]
    replay_wallet_stats_pages_processed: usize,
    #[serde(default)]
    replay_wallet_stats_wallet_cursor: Option<String>,
    #[serde(default)]
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    #[serde(default)]
    replay_wallet_stats_budget_floor_wallets: usize,
    #[serde(default)]
    replay_wallet_stats_last_partial_cycle_pages_processed: usize,
    #[serde(default)]
    replay_wallet_stats_last_partial_cycle_wallets_processed: usize,
    #[serde(default)]
    replay_wallet_stats_last_partial_cycle_elapsed_ms: u64,
    #[serde(default)]
    replay_wallet_stats_milestone_reached: bool,
    #[serde(default)]
    replay_sol_leg_reentry_pending: bool,
    #[serde(default)]
    replay_sol_leg_last_partial_cycle_pages_processed: usize,
    #[serde(default)]
    replay_sol_leg_last_partial_cycle_rows_processed: usize,
    #[serde(default)]
    replay_sol_leg_last_partial_cycle_elapsed_ms: u64,
    #[serde(default)]
    replay_sol_leg_budget_floor_pages: usize,
    #[serde(default)]
    replay_sol_leg_retained_contract_floor_pages: usize,
    #[serde(default)]
    replay_candidate_activity_backfill_required: bool,
    #[serde(default)]
    replay_candidate_activity_backfill_pending: bool,
    #[serde(default)]
    replay_candidate_activity_backfill_wallet_cursor: Option<String>,
    #[serde(default)]
    replay_exact_target_surface_wallet_cursor: Option<String>,
    #[serde(default)]
    replay_exact_target_surface_pre_row_blocked: bool,
    #[serde(default)]
    replay_exact_target_surface_staged_wallet_ids: Vec<String>,
    #[serde(default)]
    replay_exact_target_surface_staged_wallet_cursor_after: Option<String>,
    token_quality_cache: HashMap<String, quality_cache::TokenQualityResolution>,
    token_quality_progress: quality_cache::TokenQualityResolutionProgress,
    by_wallet: HashMap<String, WalletAccumulator>,
    token_states: HashMap<String, TokenRollingState>,
    token_recent_sol_trades: HashMap<String, VecDeque<SolLegTrade>>,
    pending_rug_checks: VecDeque<PendingBuyRugCheck>,
    token_pending_buy_starts: HashMap<String, VecDeque<DateTime<Utc>>>,
    completed_snapshots: Vec<WalletSnapshot>,
    #[serde(default)]
    publish_pending_requested_wallet_ids: Option<Vec<String>>,
    #[serde(default)]
    publish_pending_quality_retry_mints: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct PersistedStreamRebuildState {
    phase: DiscoveryPersistedRebuildPhase,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    prepass_rows_processed: usize,
    prepass_pages_processed: usize,
    replay_rows_processed: usize,
    replay_pages_processed: usize,
    chunks_completed: usize,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    payload: PersistedStreamRebuildPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedStreamBudgetExhaustedReason {
    TimeBudget,
    PageBudget,
}

impl PersistedStreamBudgetExhaustedReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::TimeBudget => "time_budget",
            Self::PageBudget => "page_budget",
        }
    }
}

#[derive(Debug, Clone)]
struct PersistedStreamProgressTelemetry {
    phase: DiscoveryPersistedRebuildPhase,
    collect_buy_mints_mode: CollectBuyMintsMode,
    replay_mode: ReplayMode,
    replay_subphase: Option<&'static str>,
    window_start: DateTime<Utc>,
    horizon_end: DateTime<Utc>,
    metrics_window_start: DateTime<Utc>,
    phase_cursor: Option<DiscoveryRuntimeCursor>,
    replay_wallet_stats_wallet_cursor: Option<String>,
    collect_buy_mints_cursor_token: Option<String>,
    collect_buy_mints_reconcile_source_window_start: Option<DateTime<Utc>>,
    collect_buy_mints_reconcile_source_horizon_end: Option<DateTime<Utc>>,
    collect_buy_mints_reconcile_expired_head_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_reconcile_new_tail_cursor: Option<DiscoveryRuntimeCursor>,
    collect_buy_mints_reconcile_expired_head_cursor_token: Option<String>,
    collect_buy_mints_reconcile_new_tail_cursor_token: Option<String>,
    collect_buy_mints_reconcile_expired_head_pending_mints: usize,
    collect_buy_mints_reconcile_new_tail_slice_end_token: Option<String>,
    collect_buy_mints_reconcile_new_tail_pending_mints: usize,
    prepass_rows_processed: usize,
    prepass_pages_processed: usize,
    replay_wallet_stats_complete: bool,
    replay_wallet_stats_rows_processed: usize,
    replay_wallet_stats_pages_processed: usize,
    replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    replay_wallet_stats_budget_floor_wallets: usize,
    replay_wallet_stats_last_partial_cycle_pages_processed: usize,
    replay_wallet_stats_last_partial_cycle_wallets_processed: usize,
    replay_wallet_stats_last_partial_cycle_elapsed_ms: u64,
    replay_wallet_stats_publishable_horizon_remaining_ms: Option<u64>,
    replay_wallet_stats_milestone_reached: bool,
    replay_sol_leg_reentry_pending: bool,
    replay_sol_leg_last_partial_cycle_pages_processed: usize,
    replay_sol_leg_last_partial_cycle_rows_processed: usize,
    replay_sol_leg_last_partial_cycle_elapsed_ms: u64,
    replay_sol_leg_budget_floor_pages: usize,
    replay_sol_leg_publishable_horizon_remaining_ms: Option<u64>,
    replay_sol_leg_retained_contract_floor_pages: usize,
    replay_candidate_activity_backfill_required: bool,
    replay_candidate_activity_backfill_wallet_cursor: Option<String>,
    replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    replay_rows_processed: usize,
    replay_pages_processed: usize,
    chunks_completed: usize,
    cycle_rows_processed: usize,
    cycle_pages_processed: usize,
    cycle_replay_wallet_stats_day_count_source_progress: ReplayWalletStatsDayCountSourceProgress,
    cycle_replay_wallet_stats_wallet_cursor_before: Option<String>,
    cycle_replay_wallet_stats_wallet_cursor_after: Option<String>,
    cycle_unique_buy_mints_discovered: usize,
    observed_swaps_loaded: usize,
    unique_buy_mints: usize,
    quality_next_mint_index: usize,
    quality_rpc_attempted: usize,
    quality_rpc_spent_ms: u64,
    wallets_buffered: usize,
    publish_pending_requested_wallet_count: usize,
    started_at: DateTime<Utc>,
    cycle_elapsed_ms: u64,
    total_elapsed_ms: u64,
    partial: bool,
    completed: bool,
    budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
}
