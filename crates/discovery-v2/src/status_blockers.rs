use super::status_types::{
    DiscoveryV2CoverageSample, DiscoveryV2ScanStatus, DiscoveryV2TailStatus,
};
use copybot_config::{DiscoveryConfig, ShadowConfig};

pub(super) fn blockers(
    discovery: &DiscoveryConfig,
    shadow: &ShadowConfig,
    tail: &Option<DiscoveryV2TailStatus>,
    coverage_sample: Option<&DiscoveryV2CoverageSample>,
    scan: &DiscoveryV2ScanStatus,
    candidates: &[String],
    execution_enabled: bool,
    window_minutes: u64,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let required_active_window_minutes = if discovery.min_active_days <= 1 {
        0
    } else {
        u64::from(discovery.min_active_days).saturating_mul(24 * 60)
    };
    push_if(
        &mut blockers,
        discovery_v2_float_gates_invalid(discovery, shadow),
        "discovery_v2_float_gates_invalid",
    );
    push_if(
        &mut blockers,
        required_active_window_minutes > 0 && window_minutes < required_active_window_minutes,
        "discovery_v2_active_days_unsatisfiable",
    );
    push_if(
        &mut blockers,
        !shadow.quality_gates_enabled,
        "discovery_v2_quality_gates_disabled",
    );
    push_if(
        &mut blockers,
        shadow.min_token_age_seconds == 0,
        "discovery_v2_quality_token_age_gate_disabled",
    );
    push_if(
        &mut blockers,
        shadow.min_holders == 0,
        "discovery_v2_quality_holder_gate_disabled",
    );
    push_if(
        &mut blockers,
        shadow.min_liquidity_sol <= 0.0,
        "discovery_v2_quality_liquidity_gate_disabled",
    );
    push_if(
        &mut blockers,
        shadow.min_volume_5m_sol <= 0.0,
        "discovery_v2_quality_rolling_volume_gate_disabled",
    );
    push_if(
        &mut blockers,
        shadow.min_unique_traders_5m == 0,
        "discovery_v2_quality_rolling_trader_gate_disabled",
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
        tail.as_ref().is_some_and(|status| status.future_dated),
        "observed_swaps_tail_future_dated",
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
        coverage_sample
            .as_ref()
            .is_some_and(|sample| !sample.covers_window_start),
        "observed_swaps_window_coverage_incomplete",
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
    push_if(
        &mut blockers,
        candidates.len() < discovery.follow_top_n.max(1) as usize,
        "discovery_v2_candidate_wallets_below_publish_floor",
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

fn discovery_v2_float_gates_invalid(discovery: &DiscoveryConfig, shadow: &ShadowConfig) -> bool {
    !finite_non_negative(discovery.min_leader_notional_sol)
        || !finite_non_negative(discovery.min_score)
        || !finite_ratio(discovery.min_tradable_ratio)
        || !finite_ratio(discovery.max_rug_ratio)
        || !finite_non_negative(discovery.thin_market_min_volume_sol)
        || !finite_non_negative(shadow.min_leader_notional_sol)
        || !finite_non_negative(shadow.min_liquidity_sol)
        || !finite_non_negative(shadow.min_volume_5m_sol)
}

fn finite_non_negative(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

fn finite_ratio(value: f64) -> bool {
    finite_non_negative(value) && value <= 1.0
}
