use std::collections::BTreeMap;

#[derive(Debug, Clone, Default)]
pub struct ExecutionBatchReport {
    pub attempted: u64,
    pub confirmed: u64,
    pub dropped: u64,
    pub failed: u64,
    pub skipped: u64,
    pub submit_attempted_by_route: BTreeMap<String, u64>,
    pub submit_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub submit_failed_by_route: BTreeMap<String, u64>,
    pub submit_fallback_blocked_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_policy_enabled_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_hint_used_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_hint_api_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_hint_rpc_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_price_applied_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_cu_static_fallback_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_tip_policy_enabled_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_tip_applied_by_route: BTreeMap<String, u64>,
    pub submit_dynamic_tip_static_floor_by_route: BTreeMap<String, u64>,
    pub pretrade_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub pretrade_terminal_rejected_by_route: BTreeMap<String, u64>,
    pub pretrade_failed_by_route: BTreeMap<String, u64>,
    pub confirm_confirmed_by_route: BTreeMap<String, u64>,
    pub confirm_retry_scheduled_by_route: BTreeMap<String, u64>,
    pub confirm_failed_by_route: BTreeMap<String, u64>,
    pub confirm_network_fee_rpc_by_route: BTreeMap<String, u64>,
    pub confirm_network_fee_submit_hint_by_route: BTreeMap<String, u64>,
    pub confirm_network_fee_missing_by_route: BTreeMap<String, u64>,
    pub confirm_network_fee_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_tip_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_ata_rent_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_fee_total_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_base_fee_hint_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_priority_fee_hint_lamports_sum_by_route: BTreeMap<String, u64>,
    pub confirm_latency_samples_by_route: BTreeMap<String, u64>,
    pub confirm_latency_ms_sum_by_route: BTreeMap<String, u64>,
}

pub(crate) fn bump_route_counter(counters: &mut BTreeMap<String, u64>, route: &str) {
    if route.trim().is_empty() {
        return;
    }
    let entry = counters.entry(route.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
}

pub(crate) fn accumulate_route_sum(counters: &mut BTreeMap<String, u64>, route: &str, value: u64) {
    if route.trim().is_empty() {
        return;
    }
    let entry = counters.entry(route.to_string()).or_insert(0);
    *entry = entry.saturating_add(value);
}
