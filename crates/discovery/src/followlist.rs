use super::WalletSnapshot;
use std::cmp::Ordering;

pub(super) fn rank_follow_candidates(
    snapshots: &[WalletSnapshot],
    min_score: f64,
) -> Vec<&WalletSnapshot> {
    let mut ranked: Vec<&WalletSnapshot> = snapshots
        .iter()
        .filter(|item| item.eligible && item.score >= min_score)
        .collect();
    ranked.sort_by(|a, b| cmp_score_then_trades(a, b));
    ranked
}

pub(super) fn desired_wallets(ranked: &[&WalletSnapshot], follow_top_n: u32) -> Vec<String> {
    ranked
        .iter()
        .take(follow_top_n as usize)
        .map(|item| item.wallet_id.clone())
        .collect()
}

pub(super) fn top_wallet_labels(ranked: &[&WalletSnapshot], limit: usize) -> Vec<String> {
    ranked
        .iter()
        .take(limit)
        .map(|item| {
            format!(
                "{}:{:.3}:t{:.2}:r{:.2}:b{}",
                item.wallet_id, item.score, item.tradable_ratio, item.rug_ratio, item.buy_total
            )
        })
        .collect::<Vec<_>>()
}

fn cmp_score_then_trades(a: &WalletSnapshot, b: &WalletSnapshot) -> Ordering {
    b.score
        .partial_cmp(&a.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| b.trades.cmp(&a.trades))
        .then_with(|| a.wallet_id.cmp(&b.wallet_id))
}
