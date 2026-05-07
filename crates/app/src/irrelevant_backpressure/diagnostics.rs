use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IrrelevantObservedSwapEnqueueOutcome {
    Enqueued,
    PendingWriterBackpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IrrelevantObservedSwapBackpressureSourceBranch {
    Unclassified,
    NotFollowed,
}

impl IrrelevantObservedSwapBackpressureSourceBranch {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Unclassified => "irrelevant_unclassified",
            Self::NotFollowed => "irrelevant_not_followed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IrrelevantObservedSwapBackpressureDiagnostics {
    pub(crate) irrelevant_branch: &'static str,
    pub(crate) discovery_critical_irrelevant_persistence: bool,
    pub(crate) zero_universe_empty_target_noncritical_context: bool,
    pub(crate) followed_wallet_count: usize,
    pub(crate) open_shadow_lot_count: usize,
    pub(crate) discovery_critical_target_buy_mints_count: usize,
    pub(crate) pending_irrelevant_swap_queue_depth: usize,
    pub(crate) writer_pending_requests: usize,
    pub(crate) yellowstone_output_queue_depth: u64,
}

pub(crate) fn snapshot_irrelevant_observed_swap_backpressure_diagnostics(
    branch: IrrelevantObservedSwapBackpressureSourceBranch,
    discovery_critical_irrelevant_persistence: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_snapshot: Option<IngestionRuntimeSnapshot>,
) -> IrrelevantObservedSwapBackpressureDiagnostics {
    IrrelevantObservedSwapBackpressureDiagnostics {
        irrelevant_branch: branch.as_str(),
        discovery_critical_irrelevant_persistence,
        zero_universe_empty_target_noncritical_context:
            zero_universe_empty_target_noncritical_irrelevant_context(
                discovery_critical_irrelevant_persistence,
                follow_snapshot,
                open_shadow_lots,
                shadow_strategy_fail_closed,
                discovery_critical_target_buy_mints,
            ),
        followed_wallet_count: follow_snapshot.active.len(),
        open_shadow_lot_count: open_shadow_lots.len(),
        discovery_critical_target_buy_mints_count: discovery_critical_target_buy_mints.len(),
        pending_irrelevant_swap_queue_depth: pending_irrelevant_swaps.len(),
        writer_pending_requests: observed_swap_writer_snapshot.pending_requests,
        yellowstone_output_queue_depth: ingestion_snapshot
            .map(|snapshot| snapshot.yellowstone_output_queue_depth)
            .unwrap_or(0),
    }
}

pub(crate) fn warn_irrelevant_observed_swap_writer_backpressure(
    swap: &SwapEvent,
    branch: IrrelevantObservedSwapBackpressureSourceBranch,
    discovery_critical_irrelevant_persistence: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &HashSet<String>,
    pending_irrelevant_swaps: &VecDeque<PendingIrrelevantObservedSwap>,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_snapshot: Option<IngestionRuntimeSnapshot>,
) {
    let diagnostics = snapshot_irrelevant_observed_swap_backpressure_diagnostics(
        branch,
        discovery_critical_irrelevant_persistence,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
        pending_irrelevant_swaps,
        observed_swap_writer_snapshot,
        ingestion_snapshot,
    );
    warn!(
        signature = %swap.signature,
        observed_swap_irrelevant_branch = diagnostics.irrelevant_branch,
        discovery_critical_irrelevant_persistence =
            diagnostics.discovery_critical_irrelevant_persistence,
        zero_universe_empty_target_noncritical_context =
            diagnostics.zero_universe_empty_target_noncritical_context,
        followed_wallet_count = diagnostics.followed_wallet_count,
        open_shadow_lot_count = diagnostics.open_shadow_lot_count,
        discovery_critical_target_buy_mints_count =
            diagnostics.discovery_critical_target_buy_mints_count,
        pending_irrelevant_swap_queue_depth =
            diagnostics.pending_irrelevant_swap_queue_depth,
        observed_swap_writer_pending_requests = diagnostics.writer_pending_requests,
        yellowstone_output_queue_depth = diagnostics.yellowstone_output_queue_depth,
        yellowstone_output_queue_capacity = ingestion_snapshot
            .map(|snapshot| snapshot.yellowstone_output_queue_capacity)
            .unwrap_or(0),
        yellowstone_output_queue_fill_ratio = ingestion_snapshot
            .map(|snapshot| snapshot.yellowstone_output_queue_fill_ratio)
            .unwrap_or(0.0),
        "observed swap writer queue is saturated; deferring irrelevant observed swap persistence without restarting runtime"
    );
}

#[derive(Debug, Clone)]
pub(crate) struct PendingIrrelevantObservedSwap {
    pub(crate) swap: SwapEvent,
    pub(crate) discovery_critical: bool,
    pub(crate) processing_started_at: StdInstant,
    pub(crate) backpressure_started_at: StdInstant,
    pub(crate) last_backpressure_log_at: Option<StdInstant>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DiscoveryCriticalTargetBuyMintsBackpressureRefreshState {
    last_refresh_attempt_at: Option<StdInstant>,
}

impl DiscoveryCriticalTargetBuyMintsBackpressureRefreshState {
    pub(crate) fn should_refresh(self, now: StdInstant) -> bool {
        self.last_refresh_attempt_at
            .map(|last| {
                now.duration_since(last)
                    >= DISCOVERY_CRITICAL_TARGET_BUY_MINTS_BACKPRESSURE_REFRESH_INTERVAL
            })
            .unwrap_or(true)
    }

    pub(crate) fn note_refresh_attempt(&mut self, now: StdInstant) {
        self.last_refresh_attempt_at = Some(now);
    }
}
