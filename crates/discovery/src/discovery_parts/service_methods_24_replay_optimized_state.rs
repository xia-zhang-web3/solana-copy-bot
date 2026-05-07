use crate::*;

#[derive(Default)]
pub(crate) struct PersistedStreamReplayOptimizedProgress {
    pub(crate) replay_rows_processed: usize,
    pub(crate) replay_pages_processed: usize,
    pub(crate) replay_wallet_stats_rows_processed: usize,
    pub(crate) replay_wallet_stats_pages_processed: usize,
    pub(crate) replay_sol_leg_rows_processed: usize,
    pub(crate) replay_sol_leg_pages_processed: usize,
    pub(crate) replay_sol_leg_elapsed_ms: u64,
    pub(crate) replay_wallet_stats_day_count_source_progress:
        ReplayWalletStatsDayCountSourceProgress,
    pub(crate) replay_sol_leg_access_path: Option<ObservedSolLegCursorAccessPath>,
}

impl PersistedStreamReplayOptimizedProgress {
    pub(crate) fn phase_advance(
        &self,
        source_exhausted: bool,
        phase_cursor: Option<DiscoveryRuntimeCursor>,
        budget_exhausted_reason: Option<PersistedStreamBudgetExhaustedReason>,
    ) -> PersistedStreamPhaseAdvance {
        PersistedStreamPhaseAdvance {
            rows_processed: self.replay_rows_processed,
            pages_processed: self.replay_pages_processed,
            replay_wallet_stats_rows_processed: self.replay_wallet_stats_rows_processed,
            replay_wallet_stats_pages_processed: self.replay_wallet_stats_pages_processed,
            replay_sol_leg_rows_processed: self.replay_sol_leg_rows_processed,
            replay_sol_leg_pages_processed: self.replay_sol_leg_pages_processed,
            replay_sol_leg_elapsed_ms: self.replay_sol_leg_elapsed_ms,
            replay_wallet_stats_day_count_source_progress: self
                .replay_wallet_stats_day_count_source_progress,
            replay_sol_leg_access_path: self.replay_sol_leg_access_path,
            source_exhausted,
            phase_cursor,
            collect_buy_mints_cursor_token: None,
            unique_buy_mints_discovered: 0,
            budget_exhausted_reason,
        }
    }
}
