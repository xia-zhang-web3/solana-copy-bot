type DiscoveryScoringRepairMicroCommitOutcome = (
    &'static str,
    Option<DiscoveryRuntimeCursor>,
    Option<DiscoveryRuntimeCursor>,
    usize,
    bool,
    bool,
    bool,
    bool,
    usize,
    usize,
);

fn repair_micro_commit_outcome(
    reason: &'static str,
    current: Option<DiscoveryRuntimeCursor>,
    next: Option<DiscoveryRuntimeCursor>,
    rows: usize,
    reached_target: bool,
    gap_observed: bool,
    rug_deferred: bool,
    rug_batch_prefetch: bool,
    rug_exact_count: usize,
    rug_deferred_count: usize,
) -> DiscoveryScoringRepairMicroCommitOutcome {
    (
        reason,
        current,
        next,
        rows,
        reached_target,
        gap_observed,
        rug_deferred,
        rug_batch_prefetch,
        rug_exact_count,
        rug_deferred_count,
    )
}

fn repair_micro_commit_missing_current_outcome(
    reason: &'static str,
) -> DiscoveryScoringRepairMicroCommitOutcome {
    repair_micro_commit_outcome(reason, None, None, 0, false, false, false, false, 0, 0)
}

fn repair_micro_commit_current_zero_outcome(
    reason: &'static str,
    current: DiscoveryRuntimeCursor,
    reached_target: bool,
    gap_observed: bool,
) -> DiscoveryScoringRepairMicroCommitOutcome {
    repair_micro_commit_outcome(
        reason,
        Some(current.clone()),
        Some(current),
        0,
        reached_target,
        gap_observed,
        false,
        false,
        0,
        0,
    )
}
