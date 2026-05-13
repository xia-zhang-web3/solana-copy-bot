use super::status_types::DiscoveryV2ScanStatus;
use crate::metric::DiscoveryV2WalletMetric;

pub(super) fn sort_wallet_metrics(metrics: &mut [DiscoveryV2WalletMetric]) {
    metrics.sort_by(metric_rank_cmp);
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
        .max_by(|(_, left), (_, right)| metric_rank_cmp(left, right))
    else {
        return;
    };
    if metric_rank_cmp(&metric, worst).is_lt() {
        metrics[worst_index] = metric;
    }
}

fn metric_rank_cmp(
    left: &DiscoveryV2WalletMetric,
    right: &DiscoveryV2WalletMetric,
) -> std::cmp::Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| right.trades.cmp(&left.trades))
        .then_with(|| left.wallet_id.cmp(&right.wallet_id))
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
