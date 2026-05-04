#[derive(Debug, Clone)]
struct DiscoveryPublicationTruthRepairTraceContext {
    trace_id: u64,
    publication_state_exists_before: bool,
    publication_truth_complete_before: bool,
    publication_truth_fresh_before: bool,
    runtime_window_complete_before: bool,
    runtime_cursor_exists_before: bool,
    helper_deadline: Instant,
    collector: Option<DiscoveryPublicationTruthRepairTraceCollector>,
}

#[derive(Debug, Clone, Default)]
struct DiscoveryPublicationTruthRepairTraceCollector {
    events: Arc<Mutex<Vec<DiscoveryPublicationTruthRepairRegionTraceEvent>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveryPublicationTruthRepairRegionTraceEvent {
    trace_id: u64,
    region: &'static str,
    parent_region: Option<&'static str>,
    state: &'static str,
    elapsed_ms: u64,
    deadline_remaining_ms: u64,
    publication_state_exists_before: bool,
    publication_truth_complete_before: bool,
    publication_truth_fresh_before: bool,
    runtime_window_complete_before: bool,
    runtime_cursor_exists_before: bool,
    rebuild_phase: Option<&'static str>,
    rebuild_replay_subphase: Option<&'static str>,
    persisted_rebuild_restore_outcome: Option<&'static str>,
    pages_scanned: usize,
    rows_scanned: usize,
    wallets_scanned: usize,
    time_budget_exhausted: Option<bool>,
    rebuilt_target_mint_count: usize,
    state_repaired_for_resume: Option<bool>,
}

#[derive(Debug, Clone, Default)]
struct DiscoveryPublicationTruthRepairRegionTraceProgress {
    rebuild_phase: Option<&'static str>,
    rebuild_replay_subphase: Option<&'static str>,
    persisted_rebuild_restore_outcome: Option<&'static str>,
    pages_scanned: usize,
    rows_scanned: usize,
    wallets_scanned: usize,
    time_budget_exhausted: Option<bool>,
    rebuilt_target_mint_count: usize,
    state_repaired_for_resume: Option<bool>,
}

struct DiscoveryPublicationTruthRepairRegionScope<'a> {
    context: Option<&'a DiscoveryPublicationTruthRepairTraceContext>,
    region: &'static str,
    parent_region: Option<&'static str>,
    started_at: Instant,
    progress: DiscoveryPublicationTruthRepairRegionTraceProgress,
}

#[derive(Debug, Clone, Default)]
struct ResumeExactTargetBuyMintSurfaceRepairDiagnostics {
    attempted: bool,
    completed: bool,
    time_budget_exhausted: bool,
    wallet_pages: usize,
    wallet_rows: usize,
    wallet_id_page_wallets_seen: usize,
    target_buy_mints_restored: usize,
    wallet_cursor_before: Option<String>,
    wallet_cursor_after: Option<String>,
    persisted_partial_progress: bool,
    persisted_staged_pre_row_state: bool,
    persisted_blocked_state: bool,
    resumed_from_staged_pre_row_state: bool,
}

impl DiscoveryPublicationTruthRepairTraceContext {
    fn new(
        publication_state_exists_before: bool,
        publication_truth_complete_before: bool,
        publication_truth_fresh_before: bool,
        runtime_window_complete_before: bool,
        runtime_cursor_exists_before: bool,
        helper_deadline: Instant,
        collector: Option<DiscoveryPublicationTruthRepairTraceCollector>,
    ) -> Self {
        Self {
            trace_id: DISCOVERY_PUBLICATION_TRUTH_REPAIR_TRACE_ID
                .fetch_add(1, AtomicOrdering::Relaxed),
            publication_state_exists_before,
            publication_truth_complete_before,
            publication_truth_fresh_before,
            runtime_window_complete_before,
            runtime_cursor_exists_before,
            helper_deadline,
            collector,
        }
    }
}

impl DiscoveryPublicationTruthRepairTraceCollector {
    fn record(&self, event: DiscoveryPublicationTruthRepairRegionTraceEvent) {
        self.events
            .lock()
            .expect("region trace collector mutex poisoned")
            .push(event);
    }

    #[cfg(test)]
    fn snapshot(&self) -> Vec<DiscoveryPublicationTruthRepairRegionTraceEvent> {
        self.events
            .lock()
            .expect("region trace collector mutex poisoned")
            .clone()
    }
}
