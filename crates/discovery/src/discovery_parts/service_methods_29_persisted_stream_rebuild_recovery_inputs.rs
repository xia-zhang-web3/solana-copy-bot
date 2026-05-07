use super::*;

pub(super) struct PersistedStreamRebuildRecoveryInputs {
    pub(super) requested_contract: PersistedStreamPriorityRecoveryContract,
    pub(super) buffered_wallet_backlog_floor: usize,
    pub(super) buffered_wallet_floor_pages: usize,
    pub(super) replay_wallet_stats_progress_floor_pages: usize,
    pub(super) replay_wallet_stats_open_frontier_floor_pages: usize,
    pub(super) replay_wallet_stats_persistently_open_frontier: bool,
    pub(super) replay_wallet_stats_remaining_frontier_min_pages: usize,
    pub(super) replay_wallet_stats_remaining_frontier_min_wallets: usize,
    pub(super) replay_wallet_stats_frontier_saturated: bool,
    pub(super) replay_sol_leg_processed_floor_pages: usize,
    pub(super) replay_sol_leg_open_frontier_floor_pages: usize,
    pub(super) replay_sol_leg_remaining_frontier_min_pages: usize,
    pub(super) replay_sol_leg_remaining_frontier_min_rows: usize,
    pub(super) replay_sol_leg_frontier_saturated: bool,
    pub(super) replay_sol_leg_target_ms_per_page: u64,
    pub(super) replay_sol_leg_target_time_budget_before_retained_contract_floor: StdDuration,
    pub(super) replay_sol_leg_phase_page_limit_before_retained_contract_floor: usize,
    pub(super) replay_wallet_stats_target_time_budget_before_publishable_horizon_cap: StdDuration,
}

impl DiscoveryService {
    pub(super) fn persisted_stream_rebuild_recovery_inputs(
        &self,
        state: &PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        rebuild_time_budget: StdDuration,
        collect_buy_mints_phase_page_limit_override: Option<usize>,
        replay_wallet_stats_phase_page_limit_override: Option<usize>,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
    ) -> PersistedStreamRebuildRecoveryInputs {
        let requested_contract = PersistedStreamPriorityRecoveryContract {
            time_budget: rebuild_time_budget,
            collect_buy_mints_phase_page_limit_override,
            replay_wallet_stats_phase_page_limit_override,
            replay_sol_leg_phase_page_limit_override,
            reason: None,
        };
        let buffered_wallet_backlog_floor =
            Self::replay_wallet_stats_buffered_wallet_backlog_floor_wallets(state);
        let buffered_wallet_floor_pages = Self::replay_wallet_stats_buffered_wallet_floor_pages(
            fetch_limit,
            buffered_wallet_backlog_floor,
        );
        let replay_wallet_stats_progress_floor_pages =
            Self::replay_wallet_stats_progress_floor_pages(fetch_limit, state);
        let replay_wallet_stats_open_frontier_floor_pages =
            Self::replay_wallet_stats_open_frontier_floor_pages(fetch_limit, state);
        let replay_wallet_stats_catch_up_page_limit =
            Self::replay_wallet_stats_catch_up_page_limit(fetch_limit, fetch_page_limit);
        let replay_wallet_stats_persistently_open_frontier =
            replay_wallet_stats_open_frontier_floor_pages >= replay_wallet_stats_catch_up_page_limit
                && replay_wallet_stats_progress_floor_pages
                    >= replay_wallet_stats_catch_up_page_limit.saturating_mul(
                        DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_PUBLISHABLE_HORIZON_PROGRESS_MULTIPLIER,
                    );
        let replay_wallet_stats_remaining_frontier_min_pages =
            replay_wallet_stats_open_frontier_floor_pages;
        let replay_wallet_stats_remaining_frontier_min_wallets =
            replay_wallet_stats_remaining_frontier_min_pages
                .saturating_mul(Self::replay_wallet_stats_wallet_batch_size(fetch_limit));
        let replay_wallet_stats_frontier_saturated =
            Self::replay_wallet_stats_last_partial_cycle_frontier_saturated(fetch_limit, state);
        let replay_sol_leg_processed_floor_pages =
            Self::replay_sol_leg_processed_floor_pages(fetch_limit, state);
        let replay_sol_leg_open_frontier_floor_pages =
            Self::replay_sol_leg_open_frontier_floor_pages(fetch_limit, state);
        let replay_sol_leg_remaining_frontier_min_pages = replay_sol_leg_open_frontier_floor_pages;
        let replay_sol_leg_remaining_frontier_min_rows =
            replay_sol_leg_remaining_frontier_min_pages.saturating_mul(fetch_limit);
        let replay_sol_leg_frontier_saturated =
            Self::replay_sol_leg_last_partial_cycle_frontier_saturated(fetch_limit, state);
        let replay_sol_leg_target_ms_per_page =
            self.replay_sol_leg_target_ms_per_page(fetch_page_limit, state);
        let replay_sol_leg_target_time_budget_before_retained_contract_floor =
            Self::deep_replay_sol_leg_target_time_budget(
                requested_contract.time_budget.max(StdDuration::from_millis(
                    DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MIN_TIME_BUDGET_MS,
                )),
                replay_sol_leg_processed_floor_pages
                    .saturating_add(replay_sol_leg_open_frontier_floor_pages)
                    .max(replay_sol_leg_processed_floor_pages),
                replay_sol_leg_target_ms_per_page,
            );
        let replay_sol_leg_phase_page_limit_before_retained_contract_floor = self
            .replay_sol_leg_repair_phase_page_limit(
                fetch_page_limit,
                replay_sol_leg_target_time_budget_before_retained_contract_floor,
            );
        let replay_wallet_stats_target_time_budget_before_publishable_horizon_cap =
            Self::deep_replay_wallet_stats_target_time_budget(
                requested_contract.time_budget.max(StdDuration::from_millis(
                    DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MIN_TIME_BUDGET_MS,
                )),
                replay_wallet_stats_progress_floor_pages
                    .saturating_add(replay_wallet_stats_open_frontier_floor_pages)
                    .max(replay_wallet_stats_progress_floor_pages),
                Self::replay_wallet_stats_target_ms_per_page(state),
            );
        PersistedStreamRebuildRecoveryInputs {
            requested_contract,
            buffered_wallet_backlog_floor,
            buffered_wallet_floor_pages,
            replay_wallet_stats_progress_floor_pages,
            replay_wallet_stats_open_frontier_floor_pages,
            replay_wallet_stats_persistently_open_frontier,
            replay_wallet_stats_remaining_frontier_min_pages,
            replay_wallet_stats_remaining_frontier_min_wallets,
            replay_wallet_stats_frontier_saturated,
            replay_sol_leg_processed_floor_pages,
            replay_sol_leg_open_frontier_floor_pages,
            replay_sol_leg_remaining_frontier_min_pages,
            replay_sol_leg_remaining_frontier_min_rows,
            replay_sol_leg_frontier_saturated,
            replay_sol_leg_target_ms_per_page,
            replay_sol_leg_target_time_budget_before_retained_contract_floor,
            replay_sol_leg_phase_page_limit_before_retained_contract_floor,
            replay_wallet_stats_target_time_budget_before_publishable_horizon_cap,
        }
    }
}
