#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PersistedStreamPriorityRecoveryContract {
    time_budget: StdDuration,
    collect_buy_mints_phase_page_limit_override: Option<usize>,
    replay_wallet_stats_phase_page_limit_override: Option<usize>,
    replay_sol_leg_phase_page_limit_override: Option<usize>,
    reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicationStatePersistOutcome {
    runtime_mode: DiscoveryRuntimeMode,
    published_universe_persisted: bool,
    write_attempted: bool,
    healthy_publish_refused: bool,
    carry_forward_happened: bool,
    effective_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunCyclePublicationBoundaryDiagnostics {
    prepared_cycle_state: &'static str,
    publish_due: bool,
    persisted_rebuild_checkpoint_exists: bool,
    replay_incomplete: bool,
    persisted_rebuild_phase: Option<&'static str>,
    publishable_checkpoint_blocker: Option<&'static str>,
    persist_publication_state_called: bool,
}
