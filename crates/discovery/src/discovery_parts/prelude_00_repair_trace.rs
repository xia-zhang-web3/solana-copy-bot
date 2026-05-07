use super::*;

#[derive(Debug, Clone)]
pub(crate) struct DiscoveryPublicationTruthRepairTraceContext {
    pub(crate) trace_id: u64,
    pub(crate) publication_state_exists_before: bool,
    pub(crate) publication_truth_complete_before: bool,
    pub(crate) publication_truth_fresh_before: bool,
    pub(crate) runtime_window_complete_before: bool,
    pub(crate) runtime_cursor_exists_before: bool,
    pub(crate) helper_deadline: Instant,
    pub(crate) collector: Option<DiscoveryPublicationTruthRepairTraceCollector>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoveryPublicationTruthRepairTraceCollector {
    pub(crate) events: Arc<Mutex<Vec<DiscoveryPublicationTruthRepairRegionTraceEvent>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveryPublicationTruthRepairRegionTraceEvent {
    pub(crate) trace_id: u64,
    pub(crate) region: &'static str,
    pub(crate) parent_region: Option<&'static str>,
    pub(crate) state: &'static str,
    pub(crate) elapsed_ms: u64,
    pub(crate) deadline_remaining_ms: u64,
    pub(crate) publication_state_exists_before: bool,
    pub(crate) publication_truth_complete_before: bool,
    pub(crate) publication_truth_fresh_before: bool,
    pub(crate) runtime_window_complete_before: bool,
    pub(crate) runtime_cursor_exists_before: bool,
    pub(crate) rebuild_phase: Option<&'static str>,
    pub(crate) rebuild_replay_subphase: Option<&'static str>,
    pub(crate) persisted_rebuild_restore_outcome: Option<&'static str>,
    pub(crate) pages_scanned: usize,
    pub(crate) rows_scanned: usize,
    pub(crate) wallets_scanned: usize,
    pub(crate) time_budget_exhausted: Option<bool>,
    pub(crate) rebuilt_target_mint_count: usize,
    pub(crate) state_repaired_for_resume: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DiscoveryPublicationTruthRepairRegionTraceProgress {
    pub(crate) rebuild_phase: Option<&'static str>,
    pub(crate) rebuild_replay_subphase: Option<&'static str>,
    pub(crate) persisted_rebuild_restore_outcome: Option<&'static str>,
    pub(crate) pages_scanned: usize,
    pub(crate) rows_scanned: usize,
    pub(crate) wallets_scanned: usize,
    pub(crate) time_budget_exhausted: Option<bool>,
    pub(crate) rebuilt_target_mint_count: usize,
    pub(crate) state_repaired_for_resume: Option<bool>,
}

pub(crate) struct DiscoveryPublicationTruthRepairRegionScope<'a> {
    pub(crate) context: Option<&'a DiscoveryPublicationTruthRepairTraceContext>,
    pub(crate) region: &'static str,
    pub(crate) parent_region: Option<&'static str>,
    pub(crate) started_at: Instant,
    pub(crate) progress: DiscoveryPublicationTruthRepairRegionTraceProgress,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResumeExactTargetBuyMintSurfaceRepairDiagnostics {
    pub(crate) attempted: bool,
    pub(crate) completed: bool,
    pub(crate) time_budget_exhausted: bool,
    pub(crate) wallet_pages: usize,
    pub(crate) wallet_rows: usize,
    pub(crate) wallet_id_page_wallets_seen: usize,
    pub(crate) target_buy_mints_restored: usize,
    pub(crate) wallet_cursor_before: Option<String>,
    pub(crate) wallet_cursor_after: Option<String>,
    pub(crate) persisted_partial_progress: bool,
    pub(crate) persisted_staged_pre_row_state: bool,
    pub(crate) persisted_blocked_state: bool,
    pub(crate) resumed_from_staged_pre_row_state: bool,
}

impl DiscoveryPublicationTruthRepairTraceContext {
    pub(crate) fn new(
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
    pub(crate) fn record(&self, event: DiscoveryPublicationTruthRepairRegionTraceEvent) {
        self.events
            .lock()
            .expect("region trace collector mutex poisoned")
            .push(event);
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<DiscoveryPublicationTruthRepairRegionTraceEvent> {
        self.events
            .lock()
            .expect("region trace collector mutex poisoned")
            .clone()
    }
}
