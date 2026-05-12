use super::status_types::DiscoveryV2ScanStatus;
use crate::metric::DiscoveryV2WalletMetric;

pub(super) fn sort_wallet_metrics(metrics: &mut [DiscoveryV2WalletMetric]) {
    metrics.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.trades.cmp(&left.trades))
            .then_with(|| left.wallet_id.cmp(&right.wallet_id))
    });
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
