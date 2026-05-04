#[derive(Debug, Clone, Default)]
struct DiscoveryRecentRawJournalSafetySettleState {
    clean_since: Option<StdInstant>,
    consecutive_clean_checks: u32,
}

impl DiscoveryRecentRawJournalSafetySettleState {
    fn reset(&mut self) {
        self.clean_since = None;
        self.consecutive_clean_checks = 0;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiscoveryRecentRawJournalSafetySettleGate {
    should_start: bool,
    reason: Option<&'static str>,
    clean_since: Option<StdInstant>,
    consecutive_clean_checks: u32,
    stable_for: Option<StdDuration>,
}

fn discovery_recent_raw_journal_safety_settle_start_gate(
    settle_state: &mut DiscoveryRecentRawJournalSafetySettleState,
    now: StdInstant,
) -> DiscoveryRecentRawJournalSafetySettleGate {
    let clean_since = *settle_state.clean_since.get_or_insert(now);
    settle_state.consecutive_clean_checks = settle_state.consecutive_clean_checks.saturating_add(1);
    let stable_for = now.saturating_duration_since(clean_since);
    let should_start = settle_state.consecutive_clean_checks
        >= DISCOVERY_RECENT_RAW_JOURNAL_SETTLE_CONSECUTIVE_CHECKS
        || stable_for >= DISCOVERY_RECENT_RAW_JOURNAL_SETTLE_WINDOW;

    DiscoveryRecentRawJournalSafetySettleGate {
        should_start,
        reason: (!should_start)
            .then_some(DISCOVERY_CYCLE_DEFERRED_DUE_TO_RECENT_RAW_JOURNAL_SETTLE_WINDOW),
        clean_since: Some(clean_since),
        consecutive_clean_checks: settle_state.consecutive_clean_checks,
        stable_for: Some(stable_for),
    }
}

fn discovery_recent_raw_journal_abort_reason_from_gates(
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    persisted_lag_gate: &DiscoveryRecentRawJournalPersistedLagGate,
) -> Option<&'static str> {
    if discovery_recent_raw_journal_backlog_defer_reason(observed_swap_writer_snapshot).is_some() {
        return Some(DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_BACKLOG);
    }
    persisted_lag_gate
        .reason
        .map(|_| DISCOVERY_CYCLE_ABORTED_DUE_TO_RECENT_RAW_JOURNAL_PERSISTED_LAG)
}
