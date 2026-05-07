use super::*;

pub(crate) fn should_buffer_backpressured_irrelevant_observed_swap(
    discovery_critical: bool,
    _follow_snapshot: &FollowSnapshot,
    _open_shadow_lots: &HashSet<(String, String)>,
    _shadow_strategy_fail_closed: bool,
) -> bool {
    discovery_critical
}

pub(crate) fn should_preemptively_drop_noncritical_irrelevant_observed_swap_under_output_pressure(
    discovery_critical: bool,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    ingestion_snapshot: Option<IngestionRuntimeSnapshot>,
) -> bool {
    if discovery_critical {
        return false;
    }
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return false;
    }
    let Some(ingestion_snapshot) = ingestion_snapshot else {
        return false;
    };
    ingestion_snapshot.yellowstone_output_queue_capacity > 0
        && ingestion_snapshot.yellowstone_output_queue_fill_ratio
            >= NONCRITICAL_IRRELEVANT_OUTPUT_PRESSURE_DROP_MIN_FILL_RATIO
}
