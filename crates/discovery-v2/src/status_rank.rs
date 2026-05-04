fn sort_wallet_metrics(metrics: &mut [DiscoveryV2WalletMetric]) {
    metrics.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.trades.cmp(&left.trades))
            .then_with(|| left.wallet_id.cmp(&right.wallet_id))
    });
}

fn candidate_wallets(
    discovery: &DiscoveryConfig,
    metrics: &[DiscoveryV2WalletMetric],
) -> Vec<String> {
    metrics
        .iter()
        .filter(|metric| metric.eligible && metric.score >= discovery.min_score)
        .take(discovery.follow_top_n.max(1) as usize)
        .map(|metric| metric.wallet_id.clone())
        .collect()
}

fn scan_status(
    max_rows: usize,
    time_budget_ms: u64,
    accepted_rows: usize,
    time_budget_exhausted: bool,
    metrics: &[DiscoveryV2WalletMetric],
) -> DiscoveryV2ScanStatus {
    let max_rows_exhausted = accepted_rows > max_rows;
    DiscoveryV2ScanStatus {
        max_rows,
        time_budget_ms,
        rows_scanned: accepted_rows.min(max_rows),
        unique_wallets: metrics.len(),
        max_rows_exhausted,
        time_budget_exhausted,
        budget_exhausted: max_rows_exhausted || time_budget_exhausted,
    }
}
