use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistedStreamPriorityRecoveryContract {
    pub(crate) time_budget: StdDuration,
    pub(crate) collect_buy_mints_phase_page_limit_override: Option<usize>,
    pub(crate) replay_wallet_stats_phase_page_limit_override: Option<usize>,
    pub(crate) replay_sol_leg_phase_page_limit_override: Option<usize>,
    pub(crate) reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationStatePersistOutcome {
    pub(crate) runtime_mode: DiscoveryRuntimeMode,
    pub(crate) published_universe_persisted: bool,
    pub(crate) write_attempted: bool,
    pub(crate) healthy_publish_refused: bool,
    pub(crate) carry_forward_happened: bool,
    pub(crate) effective_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunCyclePublicationBoundaryDiagnostics {
    pub(crate) prepared_cycle_state: &'static str,
    pub(crate) publish_due: bool,
    pub(crate) persisted_rebuild_checkpoint_exists: bool,
    pub(crate) replay_incomplete: bool,
    pub(crate) persisted_rebuild_phase: Option<&'static str>,
    pub(crate) publishable_checkpoint_blocker: Option<&'static str>,
    pub(crate) persist_publication_state_called: bool,
}
