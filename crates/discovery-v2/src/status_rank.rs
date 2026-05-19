use super::status_types::DiscoveryV2ScanStatus;
use crate::maturity::{maturity_tier, MaturityTier};
use crate::metric::DiscoveryV2WalletMetric;
use copybot_config::DiscoveryConfig;

pub(super) fn sort_wallet_metrics(
    metrics: &mut [DiscoveryV2WalletMetric],
    discovery: &DiscoveryConfig,
) {
    metrics.sort_by(|left, right| metric_rank_cmp(left, right, discovery.maturity_min_active_days));
}

pub(super) fn retain_top_wallet_metric(
    metrics: &mut Vec<DiscoveryV2WalletMetric>,
    metric: DiscoveryV2WalletMetric,
    limit: usize,
) {
    if limit == 0 {
        return;
    }
    if metrics.len() < limit {
        metrics.push(metric);
        return;
    }
    let Some((worst_index, worst)) = metrics
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| base_metric_rank_cmp(left, right))
    else {
        return;
    };
    if base_metric_rank_cmp(&metric, worst).is_lt() {
        metrics[worst_index] = metric;
    }
}

fn metric_rank_cmp(
    left: &DiscoveryV2WalletMetric,
    right: &DiscoveryV2WalletMetric,
    maturity_min_active_days: u32,
) -> std::cmp::Ordering {
    maturity_tier_rank(left, maturity_min_active_days)
        .cmp(&maturity_tier_rank(right, maturity_min_active_days))
        .then_with(|| base_metric_rank_cmp(left, right))
}

fn base_metric_rank_cmp(
    left: &DiscoveryV2WalletMetric,
    right: &DiscoveryV2WalletMetric,
) -> std::cmp::Ordering {
    right
        .selection_score
        .partial_cmp(&left.selection_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| right.maturity_active_days.cmp(&left.maturity_active_days))
        .then_with(|| right.trades.cmp(&left.trades))
        .then_with(|| left.wallet_id.cmp(&right.wallet_id))
}

fn maturity_tier_rank(metric: &DiscoveryV2WalletMetric, maturity_min_active_days: u32) -> u8 {
    match maturity_tier(metric, maturity_min_active_days) {
        MaturityTier::Primary => 0,
        MaturityTier::Secondary => 1,
        MaturityTier::Emergency => 2,
    }
}

pub(super) fn scan_status(
    max_rows: usize,
    time_budget_ms: u64,
    accepted_rows: usize,
    time_budget_exhausted: bool,
    unique_wallets: usize,
) -> DiscoveryV2ScanStatus {
    let max_rows_exhausted = accepted_rows > max_rows;
    DiscoveryV2ScanStatus {
        max_rows,
        time_budget_ms,
        rows_scanned: accepted_rows.min(max_rows),
        unique_wallets,
        max_rows_exhausted,
        time_budget_exhausted,
        budget_exhausted: max_rows_exhausted || time_budget_exhausted,
    }
}
