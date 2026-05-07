use super::*;

pub(super) struct PersistedStreamProgressLogDerived {
    pub(super) replay_wallet_stats_current_observed_wallet_floor: usize,
    pub(super) replay_wallet_stats_progress_floor_pages: usize,
    pub(super) replay_wallet_stats_wallet_batch_size: usize,
    pub(super) replay_wallet_stats_target_ms_per_page: u64,
    pub(super) replay_wallet_stats_last_partial_cycle_frontier_saturated: bool,
    pub(super) replay_wallet_stats_open_frontier_floor_pages: usize,
    pub(super) replay_wallet_stats_persistently_open_frontier: bool,
    pub(super) replay_wallet_stats_remaining_frontier_min_pages: usize,
    pub(super) replay_wallet_stats_remaining_frontier_min_wallets: usize,
    pub(super) replay_sol_leg_last_partial_cycle_frontier_saturated: bool,
    pub(super) replay_sol_leg_open_frontier_floor_pages: usize,
    pub(super) replay_sol_leg_remaining_frontier_min_pages: usize,
    pub(super) replay_sol_leg_remaining_frontier_min_rows: usize,
    pub(super) replay_sol_leg_target_ms_per_page: u64,
}

impl DiscoveryService {
    pub(super) fn persisted_stream_progress_log_derived(
        &self,
        telemetry: &PersistedStreamProgressTelemetry,
    ) -> PersistedStreamProgressLogDerived {
        let replay_wallet_stats_current_observed_wallet_floor = telemetry.wallets_buffered.max(
            telemetry
                .replay_wallet_stats_day_count_source_progress
                .fast_path_wallets_processed
                .saturating_add(
                    telemetry
                        .replay_wallet_stats_day_count_source_progress
                        .fallback_wallets_processed,
                ),
        );
        let replay_wallet_stats_buffered_wallet_floor_pages =
            Self::replay_wallet_stats_buffered_wallet_floor_pages(
                self.config.max_fetch_swaps_per_cycle,
                telemetry
                    .replay_wallet_stats_budget_floor_wallets
                    .max(replay_wallet_stats_current_observed_wallet_floor),
            );
        let replay_wallet_stats_progress_floor_pages = telemetry
            .replay_wallet_stats_pages_processed
            .max(replay_wallet_stats_buffered_wallet_floor_pages);
        let replay_wallet_stats_wallet_batch_size =
            Self::replay_wallet_stats_wallet_batch_size(self.config.max_fetch_swaps_per_cycle);
        let replay_wallet_stats_target_ms_per_page = if telemetry
            .replay_wallet_stats_last_partial_cycle_pages_processed
            > 0
            && telemetry.replay_wallet_stats_last_partial_cycle_elapsed_ms > 0
        {
            telemetry
                .replay_wallet_stats_last_partial_cycle_elapsed_ms
                .div_ceil(telemetry.replay_wallet_stats_last_partial_cycle_pages_processed as u64)
                .clamp(
                    DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_TARGET_MS_PER_PAGE,
                    DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MAX_OBSERVED_MS_PER_PAGE,
                )
        } else {
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_TARGET_MS_PER_PAGE
        };
        let replay_wallet_stats_last_partial_cycle_frontier_saturated = if telemetry
            .replay_wallet_stats_last_partial_cycle_pages_processed
            == 0
        {
            false
        } else if telemetry.replay_wallet_stats_last_partial_cycle_wallets_processed > 0 {
            telemetry.replay_wallet_stats_last_partial_cycle_wallets_processed
                >= telemetry
                    .replay_wallet_stats_last_partial_cycle_pages_processed
                    .saturating_mul(replay_wallet_stats_wallet_batch_size)
        } else {
            telemetry
                .replay_wallet_stats_day_count_source_progress
                .total_wallets_processed()
                .saturating_mul(
                    DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_DENOMINATOR,
                )
                >= telemetry
                    .replay_wallet_stats_pages_processed
                    .max(1)
                    .saturating_mul(replay_wallet_stats_wallet_batch_size)
                    .saturating_mul(
                        DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_NUMERATOR,
                    )
        };
        let replay_wallet_stats_open_frontier_floor_pages =
            if replay_wallet_stats_last_partial_cycle_frontier_saturated {
                telemetry.replay_wallet_stats_last_partial_cycle_pages_processed
            } else {
                0
            };
        let replay_wallet_stats_catch_up_page_limit = Self::replay_wallet_stats_catch_up_page_limit(
            self.config.max_fetch_swaps_per_cycle,
            self.config.max_fetch_pages_per_cycle,
        );
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
                .saturating_mul(replay_wallet_stats_wallet_batch_size);
        let replay_sol_leg_last_partial_cycle_frontier_saturated = if telemetry
            .replay_sol_leg_last_partial_cycle_pages_processed
            == 0
        {
            false
        } else if telemetry.replay_sol_leg_last_partial_cycle_rows_processed == 0 {
            false
        } else {
            telemetry
                .replay_sol_leg_last_partial_cycle_rows_processed
                .saturating_mul(
                    DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_DENOMINATOR,
                )
                >= telemetry
                    .replay_sol_leg_last_partial_cycle_pages_processed
                    .max(1)
                    .saturating_mul(self.config.max_fetch_swaps_per_cycle.max(1))
                    .saturating_mul(
                        DISCOVERY_PUBLICATION_TRUTH_REPLAY_WALLET_STATS_FRONTIER_SATURATION_NUMERATOR,
                    )
        };
        let replay_sol_leg_open_frontier_floor_pages =
            if replay_sol_leg_last_partial_cycle_frontier_saturated {
                telemetry.replay_sol_leg_last_partial_cycle_pages_processed
            } else {
                0
            };
        let replay_sol_leg_remaining_frontier_min_pages = replay_sol_leg_open_frontier_floor_pages;
        let replay_sol_leg_remaining_frontier_min_rows =
            replay_sol_leg_remaining_frontier_min_pages
                .saturating_mul(self.config.max_fetch_swaps_per_cycle.max(1));
        let replay_sol_leg_target_ms_per_page =
            Self::replay_sol_leg_target_ms_per_page_from_observed_cycle(
                self.config.fetch_time_budget_ms,
                self.config.max_fetch_pages_per_cycle,
                telemetry.replay_sol_leg_last_partial_cycle_pages_processed,
                telemetry.replay_sol_leg_last_partial_cycle_elapsed_ms,
            );
        PersistedStreamProgressLogDerived {
            replay_wallet_stats_current_observed_wallet_floor,
            replay_wallet_stats_progress_floor_pages,
            replay_wallet_stats_wallet_batch_size,
            replay_wallet_stats_target_ms_per_page,
            replay_wallet_stats_last_partial_cycle_frontier_saturated,
            replay_wallet_stats_open_frontier_floor_pages,
            replay_wallet_stats_persistently_open_frontier,
            replay_wallet_stats_remaining_frontier_min_pages,
            replay_wallet_stats_remaining_frontier_min_wallets,
            replay_sol_leg_last_partial_cycle_frontier_saturated,
            replay_sol_leg_open_frontier_floor_pages,
            replay_sol_leg_remaining_frontier_min_pages,
            replay_sol_leg_remaining_frontier_min_rows,
            replay_sol_leg_target_ms_per_page,
        }
    }
}
