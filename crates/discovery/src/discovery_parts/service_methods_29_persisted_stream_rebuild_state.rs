use super::*;

pub(crate) struct PersistedStreamAdvanceCycleState {
    pub(crate) started: Instant,
    pub(crate) deadline: Instant,
    pub(crate) rows_processed: usize,
    pub(crate) pages_processed: usize,
    pub(crate) replay_sol_leg_rows_processed: usize,
    pub(crate) replay_sol_leg_pages_processed: usize,
    pub(crate) replay_sol_leg_elapsed_ms: u64,
    pub(crate) replay_wallet_stats_day_count_source_progress:
        ReplayWalletStatsDayCountSourceProgress,
    pub(crate) replay_wallet_stats_wallet_cursor_before: Option<String>,
    pub(crate) unique_buy_mints_discovered: usize,
    pub(crate) replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
    pub(crate) started_in_replay_sol_leg: bool,
    pub(crate) replay_sol_leg_phase_page_limit: Option<usize>,
}

pub(crate) enum PersistedStreamRebuildCycleLoopOutcome {
    Completed {
        outcome: PersistedStreamRebuildAdvanceOutcome,
    },
    InProgress {
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    },
}

impl PersistedStreamAdvanceCycleState {
    pub(crate) fn new(
        state: &PersistedStreamRebuildState,
        rebuild_time_budget: StdDuration,
        fetch_page_limit: usize,
        replay_sol_leg_phase_page_limit_override: Option<usize>,
    ) -> Self {
        let started = Instant::now();
        let started_in_replay_sol_leg = state.phase == DiscoveryPersistedRebuildPhase::Replay
            && state.payload.replay_wallet_stats_complete
            && !state.payload.replay_candidate_activity_backfill_pending;
        let replay_sol_leg_phase_page_limit = started_in_replay_sol_leg
            .then_some(replay_sol_leg_phase_page_limit_override.unwrap_or(fetch_page_limit));
        Self {
            started,
            deadline: started + rebuild_time_budget,
            rows_processed: 0,
            pages_processed: 0,
            replay_sol_leg_rows_processed: 0,
            replay_sol_leg_pages_processed: 0,
            replay_sol_leg_elapsed_ms: 0,
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress::default(),
            replay_wallet_stats_wallet_cursor_before: state
                .payload
                .replay_wallet_stats_wallet_cursor
                .clone(),
            unique_buy_mints_discovered: 0,
            replay_sol_leg_access_path: None,
            started_in_replay_sol_leg,
            replay_sol_leg_phase_page_limit,
        }
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    pub(crate) fn absorb_phase_advance(&mut self, phase_advance: &PersistedStreamPhaseAdvance) {
        self.rows_processed = self
            .rows_processed
            .saturating_add(phase_advance.rows_processed)
            .saturating_add(phase_advance.replay_wallet_stats_rows_processed);
        self.pages_processed = self
            .pages_processed
            .saturating_add(phase_advance.pages_processed)
            .saturating_add(phase_advance.replay_wallet_stats_pages_processed);
        self.replay_sol_leg_rows_processed = self
            .replay_sol_leg_rows_processed
            .saturating_add(phase_advance.replay_sol_leg_rows_processed);
        self.replay_sol_leg_pages_processed = self
            .replay_sol_leg_pages_processed
            .saturating_add(phase_advance.replay_sol_leg_pages_processed);
        self.replay_sol_leg_elapsed_ms = self
            .replay_sol_leg_elapsed_ms
            .saturating_add(phase_advance.replay_sol_leg_elapsed_ms);
        self.replay_wallet_stats_day_count_source_progress
            .merge(phase_advance.replay_wallet_stats_day_count_source_progress);
        self.unique_buy_mints_discovered = self
            .unique_buy_mints_discovered
            .saturating_add(phase_advance.unique_buy_mints_discovered);
        if phase_advance.replay_sol_leg_access_path.is_some() {
            self.replay_sol_leg_access_path = phase_advance.replay_sol_leg_access_path;
        }
    }

    pub(crate) fn telemetry_input(
        &self,
        now: DateTime<Utc>,
        partial: bool,
        completed: bool,
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    ) -> PersistedStreamCycleTelemetryInput {
        PersistedStreamCycleTelemetryInput {
            now,
            cycle_rows_processed: self.rows_processed,
            cycle_pages_processed: self.pages_processed,
            cycle_replay_wallet_stats_day_count_source_progress: self
                .replay_wallet_stats_day_count_source_progress,
            cycle_replay_wallet_stats_wallet_cursor_before: self
                .replay_wallet_stats_wallet_cursor_before
                .clone(),
            cycle_unique_buy_mints_discovered: self.unique_buy_mints_discovered,
            cycle_replay_sol_leg_access_path: self.replay_sol_leg_access_path,
            cycle_elapsed_ms: self.elapsed_ms(),
            partial,
            completed,
            budget_exhausted_reason,
        }
    }
}
