fn blockers(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    tail: &Option<DiscoveryV2TailStatus>,
    coverage_sample: Option<&DiscoveryV2CoverageSample>,
    scan: &DiscoveryV2ScanStatus,
    candidates: &[String],
    execution_enabled: bool,
) -> Vec<String> {
    let mut blockers = Vec::new();
    push_if(
        &mut blockers,
        !shadow.quality_gates_enabled,
        "discovery_v2_quality_gates_disabled",
    );
    push_if(
        &mut blockers,
        !discovery.require_open_positions_for_publication,
        "discovery_v2_open_position_gate_disabled",
    );
    push_if(
        &mut blockers,
        discovery.max_rug_ratio >= 1.0,
        "discovery_v2_rug_gate_disabled",
    );
    push_if(
        &mut blockers,
        discovery.thin_market_min_volume_sol <= 0.0,
        "discovery_v2_thin_market_volume_gate_disabled",
    );
    push_if(
        &mut blockers,
        discovery.thin_market_min_unique_traders == 0,
        "discovery_v2_thin_market_trader_gate_disabled",
    );
    push_if(
        &mut blockers,
        !tail.as_ref().is_some_and(|status| status.fresh),
        "observed_swaps_tail_stale_or_missing",
    );
    push_if(
        &mut blockers,
        coverage_sample.is_none(),
        "observed_swaps_window_sample_missing",
    );
    push_if(
        &mut blockers,
        scan.rows_scanned == 0,
        "observed_swaps_window_scan_empty",
    );
    push_if(
        &mut blockers,
        candidates.is_empty(),
        "discovery_v2_candidate_wallets_empty",
    );
    push_if(&mut blockers, execution_enabled, "execution_enabled");
    push_if(
        &mut blockers,
        scan.budget_exhausted,
        "discovery_v2_scan_budget_exhausted",
    );
    blockers
}

fn push_if(blockers: &mut Vec<String>, condition: bool, blocker: &str) {
    if condition {
        blockers.push(blocker.to_string());
    }
}
