use copybot_execution::ExecutionBatchReport;
use tracing::info;

pub(crate) fn log_execution_batch_report(report: &ExecutionBatchReport) {
    if report.attempted == 0 && report.failed == 0 {
        return;
    }

    let has_route_metrics = !report.submit_attempted_by_route.is_empty()
        || !report.submit_retry_scheduled_by_route.is_empty()
        || !report.submit_failed_by_route.is_empty()
        || !report.submit_fallback_blocked_by_route.is_empty()
        || !report.submit_dynamic_cu_policy_enabled_by_route.is_empty()
        || !report.submit_dynamic_cu_hint_used_by_route.is_empty()
        || !report.submit_dynamic_cu_price_applied_by_route.is_empty()
        || !report.submit_dynamic_cu_static_fallback_by_route.is_empty()
        || !report.submit_dynamic_tip_policy_enabled_by_route.is_empty()
        || !report.submit_dynamic_tip_applied_by_route.is_empty()
        || !report.submit_dynamic_tip_static_floor_by_route.is_empty()
        || !report.pretrade_retry_scheduled_by_route.is_empty()
        || !report.pretrade_terminal_rejected_by_route.is_empty()
        || !report.pretrade_failed_by_route.is_empty()
        || !report.confirm_confirmed_by_route.is_empty()
        || !report.confirm_retry_scheduled_by_route.is_empty()
        || !report.confirm_failed_by_route.is_empty()
        || !report.confirm_network_fee_rpc_by_route.is_empty()
        || !report.confirm_network_fee_submit_hint_by_route.is_empty()
        || !report.confirm_network_fee_missing_by_route.is_empty()
        || !report.confirm_network_fee_lamports_sum_by_route.is_empty()
        || !report.confirm_tip_lamports_sum_by_route.is_empty()
        || !report.confirm_ata_rent_lamports_sum_by_route.is_empty()
        || !report.confirm_fee_total_lamports_sum_by_route.is_empty()
        || !report
            .confirm_base_fee_hint_lamports_sum_by_route
            .is_empty()
        || !report
            .confirm_priority_fee_hint_lamports_sum_by_route
            .is_empty()
        || !report.confirm_latency_samples_by_route.is_empty()
        || !report.confirm_latency_ms_sum_by_route.is_empty();

    info!(
        attempted = report.attempted,
        confirmed = report.confirmed,
        dropped = report.dropped,
        failed = report.failed,
        skipped = report.skipped,
        submit_attempted_by_route = ?report.submit_attempted_by_route,
        submit_retry_scheduled_by_route = ?report.submit_retry_scheduled_by_route,
        submit_failed_by_route = ?report.submit_failed_by_route,
        submit_fallback_blocked_by_route = ?report.submit_fallback_blocked_by_route,
        submit_dynamic_cu_policy_enabled_by_route = ?report.submit_dynamic_cu_policy_enabled_by_route,
        submit_dynamic_cu_hint_used_by_route = ?report.submit_dynamic_cu_hint_used_by_route,
        submit_dynamic_cu_price_applied_by_route = ?report.submit_dynamic_cu_price_applied_by_route,
        submit_dynamic_cu_static_fallback_by_route = ?report.submit_dynamic_cu_static_fallback_by_route,
        submit_dynamic_tip_policy_enabled_by_route = ?report.submit_dynamic_tip_policy_enabled_by_route,
        submit_dynamic_tip_applied_by_route = ?report.submit_dynamic_tip_applied_by_route,
        submit_dynamic_tip_static_floor_by_route = ?report.submit_dynamic_tip_static_floor_by_route,
        pretrade_retry_scheduled_by_route = ?report.pretrade_retry_scheduled_by_route,
        pretrade_terminal_rejected_by_route = ?report.pretrade_terminal_rejected_by_route,
        pretrade_failed_by_route = ?report.pretrade_failed_by_route,
        confirm_confirmed_by_route = ?report.confirm_confirmed_by_route,
        confirm_retry_scheduled_by_route = ?report.confirm_retry_scheduled_by_route,
        confirm_failed_by_route = ?report.confirm_failed_by_route,
        confirm_network_fee_rpc_by_route = ?report.confirm_network_fee_rpc_by_route,
        confirm_network_fee_submit_hint_by_route = ?report.confirm_network_fee_submit_hint_by_route,
        confirm_network_fee_missing_by_route = ?report.confirm_network_fee_missing_by_route,
        confirm_network_fee_lamports_sum_by_route = ?report.confirm_network_fee_lamports_sum_by_route,
        confirm_tip_lamports_sum_by_route = ?report.confirm_tip_lamports_sum_by_route,
        confirm_ata_rent_lamports_sum_by_route = ?report.confirm_ata_rent_lamports_sum_by_route,
        confirm_fee_total_lamports_sum_by_route = ?report.confirm_fee_total_lamports_sum_by_route,
        confirm_base_fee_hint_lamports_sum_by_route = ?report.confirm_base_fee_hint_lamports_sum_by_route,
        confirm_priority_fee_hint_lamports_sum_by_route = ?report.confirm_priority_fee_hint_lamports_sum_by_route,
        confirm_latency_samples_by_route = ?report.confirm_latency_samples_by_route,
        confirm_latency_ms_sum_by_route = ?report.confirm_latency_ms_sum_by_route,
        confirm_latency_semantics = "submit_to_runtime_observed_confirm_ms",
        has_route_metrics,
        "execution batch processed"
    );
}
