fn zero_universe_empty_target_noncritical_irrelevant_context(
    discovery_critical: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) -> bool {
    !discovery_critical
        && discovery_critical_target_buy_mints.is_empty()
        && zero_universe_fail_closed_discovery_market_context_mode(
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
        )
}

fn should_drop_zero_universe_empty_target_noncritical_irrelevant_after_best_effort_exhaustion(
    discovery_critical: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
    best_effort_budget_exhausted: bool,
) -> bool {
    best_effort_budget_exhausted
        && zero_universe_empty_target_noncritical_irrelevant_context(
            discovery_critical,
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        )
}

#[derive(Debug, Clone)]
struct ZeroUniverseEmptyTargetNoncriticalBestEffortState {
    exhausted: bool,
    exhausted_at: Option<StdInstant>,
    refill_allowed_at: Option<StdInstant>,
}

impl Default for ZeroUniverseEmptyTargetNoncriticalBestEffortState {
    fn default() -> Self {
        Self {
            exhausted: false,
            exhausted_at: None,
            refill_allowed_at: None,
        }
    }
}

impl ZeroUniverseEmptyTargetNoncriticalBestEffortState {
    fn exhausted(&self) -> bool {
        self.exhausted
    }

    fn mark_exhausted(&mut self, now: StdInstant) {
        if !self.exhausted {
            self.exhausted_at = Some(now);
            self.refill_allowed_at =
                now.checked_add(ZERO_UNIVERSE_EMPTY_TARGET_NONCRITICAL_BEST_EFFORT_REFILL_COOLDOWN);
        } else if self.refill_allowed_at.is_none() {
            self.refill_allowed_at =
                now.checked_add(ZERO_UNIVERSE_EMPTY_TARGET_NONCRITICAL_BEST_EFFORT_REFILL_COOLDOWN);
        }
        self.exhausted = true;
    }

    fn clear(&mut self) {
        self.exhausted = false;
        self.exhausted_at = None;
        self.refill_allowed_at = None;
    }

    fn clear_if_context_changed(
        &mut self,
        follow_snapshot: &FollowSnapshot,
        open_shadow_lots: &HashSet<(String, String)>,
        shadow_strategy_fail_closed: bool,
        discovery_critical_target_buy_mints: &HashSet<String>,
    ) {
        if !zero_universe_empty_target_noncritical_irrelevant_context(
            false,
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        ) {
            self.clear();
        }
    }

    fn refresh_after_writer_pressure_clears(
        &mut self,
        follow_snapshot: &FollowSnapshot,
        open_shadow_lots: &HashSet<(String, String)>,
        shadow_strategy_fail_closed: bool,
        discovery_critical_target_buy_mints: &HashSet<String>,
        writer_snapshot: &ObservedSwapWriterSnapshot,
        now: StdInstant,
    ) {
        self.clear_if_context_changed(
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        );
        if !self.exhausted
            || zero_universe_empty_target_noncritical_writer_pressure_active(writer_snapshot)
        {
            return;
        }
        let Some(refill_allowed_at) = self.refill_allowed_at else {
            return;
        };
        if now >= refill_allowed_at {
            self.clear();
        }
    }
}

fn zero_universe_empty_target_noncritical_writer_pressure_active(
    writer_snapshot: &ObservedSwapWriterSnapshot,
) -> bool {
    writer_snapshot.pending_requests > 0
        || writer_snapshot.journal_queue_depth_batches > 0
        || writer_snapshot.journal_overflow_depth_batches > 0
}

fn reset_zero_universe_empty_target_noncritical_best_effort_exhaustion_if_context_changed(
    best_effort_state: &mut ZeroUniverseEmptyTargetNoncriticalBestEffortState,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
) {
    best_effort_state.clear_if_context_changed(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
    );
}

fn pending_irrelevant_swap_queue_is_full(
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
) -> bool {
    pending_irrelevant_swaps.len() >= DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY
}

fn should_drop_backpressured_discovery_critical_irrelevant_observed_swap(
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
) -> bool {
    pending_irrelevant_swap_queue_is_full(pending_irrelevant_swaps)
}

#[cfg(test)]
fn pending_irrelevant_swap_backpressure_blocks_ingestion(
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
) -> bool {
    pending_irrelevant_swap_queue_is_full(pending_irrelevant_swaps)
}
