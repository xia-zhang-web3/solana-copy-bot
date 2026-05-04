impl DiscoveryService {
    fn deepen_persisted_stream_priority_recovery_contract_for_state_at(
        &self,
        state: &PersistedStreamRebuildState,
        fetch_limit: usize,
        fetch_page_limit: usize,
        contract: PersistedStreamPriorityRecoveryContract,
        now: Option<DateTime<Utc>>,
    ) -> PersistedStreamPriorityRecoveryContract {
        let mut contract = contract;
        if Self::state_needs_deep_replay_wallet_stats_priority_recovery_contract(state) {
            let baseline_time_budget = contract.time_budget.max(StdDuration::from_millis(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_WALLET_STATS_MIN_TIME_BUDGET_MS,
            ));
            let buffered_wallet_backlog_floor =
                Self::replay_wallet_stats_buffered_wallet_backlog_floor_wallets(state);
            let buffered_wallet_floor_pages = Self::replay_wallet_stats_buffered_wallet_floor_pages(
                fetch_limit,
                buffered_wallet_backlog_floor,
            );
            let progress_floor_pages =
                Self::replay_wallet_stats_progress_floor_pages(fetch_limit, state);
            let open_frontier_floor_pages =
                Self::replay_wallet_stats_open_frontier_floor_pages(fetch_limit, state);
            let target_ms_per_page = Self::replay_wallet_stats_target_ms_per_page(state);
            let target_floor_pages = progress_floor_pages
                .saturating_add(open_frontier_floor_pages)
                .max(progress_floor_pages);
            let mut deep_time_budget = Self::deep_replay_wallet_stats_target_time_budget(
                baseline_time_budget,
                target_floor_pages,
                target_ms_per_page,
            );
            let mut deep_reason =
                if deep_time_budget > baseline_time_budget && open_frontier_floor_pages > 0 {
                    Some("deep_replay_wallet_stats_open_frontier_backlog")
                } else if deep_time_budget > baseline_time_budget
                    && progress_floor_pages > buffered_wallet_floor_pages
                {
                    Some("deep_replay_wallet_stats_large_processed_backlog")
                } else if deep_time_budget > baseline_time_budget {
                    Some("deep_replay_wallet_stats_large_buffered_backlog")
                } else {
                    Some("deep_replay_wallet_stats_incomplete")
                };
            if let Some(remaining_publishable_horizon_ms) = now.and_then(|now| {
                self.replay_wallet_stats_remaining_publishable_horizon_ms(state, now)
            }) {
                let remaining_publishable_horizon_budget =
                    StdDuration::from_millis(remaining_publishable_horizon_ms.max(1));
                if remaining_publishable_horizon_budget < deep_time_budget {
                    deep_time_budget = remaining_publishable_horizon_budget;
                    deep_reason = Some("deep_replay_wallet_stats_publishable_horizon_cap");
                }
            }
            contract = PersistedStreamPriorityRecoveryContract {
                time_budget: deep_time_budget,
                collect_buy_mints_phase_page_limit_override: Some(
                    self.collect_buy_mints_repair_phase_page_limit(
                        fetch_limit,
                        fetch_page_limit,
                        deep_time_budget,
                    ),
                ),
                replay_wallet_stats_phase_page_limit_override: Some(
                    self.replay_wallet_stats_repair_phase_page_limit(
                        fetch_limit,
                        fetch_page_limit,
                        deep_time_budget,
                    ),
                ),
                replay_sol_leg_phase_page_limit_override: Some(
                    self.replay_sol_leg_repair_phase_page_limit(fetch_page_limit, deep_time_budget),
                ),
                reason: deep_reason,
            };
        }
        if !Self::state_needs_deep_replay_sol_leg_priority_recovery_contract(state) {
            if !Self::state_needs_deep_replay_candidate_activity_backfill_priority_recovery_contract(
                state,
            ) {
                return contract;
            }
            let baseline_time_budget = contract.time_budget.max(StdDuration::from_millis(
                DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MIN_TIME_BUDGET_MS,
            ));
            let processed_floor_pages =
                Self::replay_sol_leg_processed_floor_pages(fetch_limit, state);
            let target_ms_per_page =
                self.replay_sol_leg_target_ms_per_page(fetch_page_limit, state);
            let deep_time_budget = Self::deep_replay_sol_leg_target_time_budget(
                baseline_time_budget,
                processed_floor_pages,
                target_ms_per_page,
            );
            let deep_reason = if deep_time_budget > baseline_time_budget {
                Some("deep_replay_candidate_activity_backfill_large_processed_backlog")
            } else {
                Some("deep_replay_candidate_activity_backfill_incomplete")
            };
            return PersistedStreamPriorityRecoveryContract {
                time_budget: deep_time_budget,
                collect_buy_mints_phase_page_limit_override: Some(
                    self.collect_buy_mints_repair_phase_page_limit(
                        fetch_limit,
                        fetch_page_limit,
                        deep_time_budget,
                    ),
                ),
                replay_wallet_stats_phase_page_limit_override: Some(
                    self.replay_wallet_stats_repair_phase_page_limit(
                        fetch_limit,
                        fetch_page_limit,
                        deep_time_budget,
                    ),
                ),
                replay_sol_leg_phase_page_limit_override: Some(
                    self.replay_sol_leg_repair_phase_page_limit(fetch_page_limit, deep_time_budget),
                ),
                reason: deep_reason,
            };
        }

        let baseline_time_budget = contract.time_budget.max(StdDuration::from_millis(
            DISCOVERY_PUBLICATION_TRUTH_REPAIR_DEEP_REPLAY_SOL_LEG_MIN_TIME_BUDGET_MS,
        ));
        let processed_floor_pages = Self::replay_sol_leg_budget_floor_pages(fetch_limit, state);
        let open_frontier_floor_pages =
            Self::replay_sol_leg_open_frontier_floor_pages(fetch_limit, state);
        let target_floor_pages = processed_floor_pages
            .saturating_add(open_frontier_floor_pages)
            .max(processed_floor_pages);
        let target_ms_per_page = self.replay_sol_leg_target_ms_per_page(fetch_page_limit, state);
        let mut deep_time_budget = Self::deep_replay_sol_leg_target_time_budget(
            baseline_time_budget,
            target_floor_pages,
            target_ms_per_page,
        );
        let mut deep_reason =
            if deep_time_budget > baseline_time_budget && open_frontier_floor_pages > 0 {
                Some("deep_replay_sol_leg_open_frontier_backlog")
            } else if deep_time_budget > baseline_time_budget {
                Some("deep_replay_sol_leg_large_processed_backlog")
            } else {
                Some("deep_replay_sol_leg_incomplete")
            };
        let retained_contract_floor_pages =
            state.payload.replay_sol_leg_retained_contract_floor_pages;
        if retained_contract_floor_pages > 0 {
            let retained_contract_floor_budget = self
                .replay_sol_leg_time_budget_for_phase_page_limit_floor(
                    fetch_page_limit,
                    retained_contract_floor_pages,
                );
            if retained_contract_floor_budget > deep_time_budget {
                deep_time_budget = retained_contract_floor_budget;
                deep_reason = Some("deep_replay_sol_leg_retained_contract_floor");
            }
        }
        if let Some(remaining_publishable_horizon_ms) =
            now.and_then(|now| self.replay_sol_leg_remaining_publishable_horizon_ms(state, now))
        {
            let remaining_publishable_horizon_budget =
                StdDuration::from_millis(remaining_publishable_horizon_ms.max(1));
            let deep_time_budget_before_publishable_horizon_adjustment = deep_time_budget;
            let uncapped_frontier_budget = Self::deep_replay_sol_leg_target_time_budget_uncapped(
                baseline_time_budget,
                target_floor_pages,
                target_ms_per_page,
            );
            let publishable_horizon_budget = uncapped_frontier_budget
                .max(deep_time_budget_before_publishable_horizon_adjustment)
                .min(remaining_publishable_horizon_budget);
            if publishable_horizon_budget > deep_time_budget_before_publishable_horizon_adjustment {
                deep_time_budget = publishable_horizon_budget;
                deep_reason = Some("deep_replay_sol_leg_publishable_horizon_backlog");
            } else if publishable_horizon_budget
                < deep_time_budget_before_publishable_horizon_adjustment
            {
                deep_time_budget = publishable_horizon_budget;
                deep_reason = Some("deep_replay_sol_leg_publishable_horizon_cap");
            }
        }
        PersistedStreamPriorityRecoveryContract {
            time_budget: deep_time_budget,
            collect_buy_mints_phase_page_limit_override: Some(
                self.collect_buy_mints_repair_phase_page_limit(
                    fetch_limit,
                    fetch_page_limit,
                    deep_time_budget,
                ),
            ),
            replay_wallet_stats_phase_page_limit_override: Some(
                self.replay_wallet_stats_repair_phase_page_limit(
                    fetch_limit,
                    fetch_page_limit,
                    deep_time_budget,
                ),
            ),
            replay_sol_leg_phase_page_limit_override: Some(
                self.replay_sol_leg_repair_phase_page_limit(fetch_page_limit, deep_time_budget),
            ),
            reason: deep_reason,
        }
    }
}
