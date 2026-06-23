use crate::leader_copyability_report_db::{FollowerCloseFact, LeaderCloseFact, WalletMetric};
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::HashMap;

const MIN_STRONG_VERDICT_WALLETS: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct WalletInputs {
    pub(crate) wallet_id: String,
    pub(crate) metric: Option<WalletMetric>,
    pub(crate) leader_closes: Vec<LeaderCloseFact>,
    pub(crate) follower_closes: Vec<FollowerCloseFact>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseContextBucket {
    pub count: u64,
    pub pnl_sol: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseContextSplit {
    pub market: CloseContextBucket,
    pub stale_quote_price: CloseContextBucket,
    pub stale_market_price: CloseContextBucket,
    pub terminal: CloseContextBucket,
    pub other: CloseContextBucket,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletCopyabilityRow {
    pub wallet_id: String,
    pub discovery_rank: Option<u64>,
    pub discovery_score: Option<f64>,
    pub discovery_window_start: Option<String>,
    pub metric_leader_pnl_sol: Option<f64>,
    pub metric_win_rate: Option<f64>,
    pub metric_closed_trades: Option<u64>,
    pub metric_hold_median_seconds: Option<i64>,
    pub metric_rug_ratio: Option<f64>,
    pub leader_pnl_sol: f64,
    pub leader_trades: u64,
    pub leader_win_rate: Option<f64>,
    pub leader_hold_median_seconds: Option<f64>,
    pub follower_pnl_sol: f64,
    pub follower_trades: u64,
    pub follower_win_rate: Option<f64>,
    pub follower_gap_sol: f64,
    pub copyability_ratio: Option<f64>,
    pub close_context_split: CloseContextSplit,
    pub eligible_for_correlation: bool,
}

#[derive(Debug, Serialize)]
pub struct CorrelationSummary {
    pub eligible_wallets: u64,
    pub rank_vs_leader_pnl_spearman: Option<f64>,
    pub rank_vs_follower_pnl_spearman: Option<f64>,
    pub rank_sign_note: String,
    pub verdict: String,
}

#[derive(Debug, Serialize)]
pub struct CopyabilitySummary {
    pub metric_basis: String,
    pub caveats: Vec<String>,
    pub wallet_count: u64,
    pub correlation: CorrelationSummary,
    pub high_leader_low_copyability: Vec<WalletCopyabilityRow>,
    pub lower_rank_high_copyability: Vec<WalletCopyabilityRow>,
    pub wallets: Vec<WalletCopyabilityRow>,
}

pub(crate) fn summarize_copyability(
    inputs: Vec<WalletInputs>,
    min_leader_trades: u64,
    min_follower_trades: u64,
) -> CopyabilitySummary {
    let mut wallets = inputs
        .into_iter()
        .map(|input| wallet_row(input, min_leader_trades, min_follower_trades))
        .collect::<Vec<_>>();
    wallets.sort_by(compare_rows);
    let correlation = correlation_summary(&wallets);
    let leader_p70 = percentile(
        wallets
            .iter()
            .filter(|row| row.leader_trades >= min_leader_trades)
            .map(|row| row.leader_pnl_sol)
            .collect(),
        0.70,
    );
    let median_rank = percentile(
        wallets
            .iter()
            .filter_map(|row| row.discovery_rank.map(|rank| rank as f64))
            .collect(),
        0.50,
    );
    let high_leader_low_copyability = wallets
        .iter()
        .filter(|row| {
            row.eligible_for_correlation
                && leader_p70
                    .map(|p70| row.leader_pnl_sol >= p70)
                    .unwrap_or(false)
                && (row.follower_pnl_sol <= 0.0
                    || row
                        .copyability_ratio
                        .map(|value| value <= 0.30)
                        .unwrap_or(false))
        })
        .take(20)
        .cloned()
        .collect();
    let lower_rank_high_copyability = wallets
        .iter()
        .filter(|row| {
            row.eligible_for_correlation
                && median_rank
                    .zip(row.discovery_rank)
                    .map(|(median, rank)| rank as f64 >= median)
                    .unwrap_or(false)
                && row.leader_pnl_sol > 0.0
                && row.follower_pnl_sol > 0.0
                && row
                    .copyability_ratio
                    .map(|value| value >= 0.50)
                    .unwrap_or(true)
        })
        .take(20)
        .cloned()
        .collect();
    CopyabilitySummary {
        metric_basis: "shadow_follower_paper_relative_only".to_string(),
        caveats: caveats(),
        wallet_count: wallets.len() as u64,
        correlation,
        high_leader_low_copyability,
        lower_rank_high_copyability,
        wallets,
    }
}

fn wallet_row(
    input: WalletInputs,
    min_leader_trades: u64,
    min_follower_trades: u64,
) -> WalletCopyabilityRow {
    let metric = input.metric;
    let has_leader_closes = !input.leader_closes.is_empty();
    let leader_pnl = if has_leader_closes {
        input
            .leader_closes
            .iter()
            .map(|row| row.pnl_sol)
            .sum::<f64>()
    } else {
        metric.as_ref().map(|row| row.pnl_sol).unwrap_or(0.0)
    };
    let leader_trades = if has_leader_closes {
        input.leader_closes.len() as u64
    } else {
        metric
            .as_ref()
            .map(|row| row.closed_trades)
            .unwrap_or_default()
    };
    let follower_pnl = input
        .follower_closes
        .iter()
        .map(|row| row.pnl_sol)
        .sum::<f64>();
    let follower_trades = input.follower_closes.len() as u64;
    let split = close_context_split(&input.follower_closes);
    let follower_gap = follower_pnl - leader_pnl;
    let copyability_ratio = (leader_pnl > 0.0).then_some(follower_pnl / leader_pnl);
    WalletCopyabilityRow {
        wallet_id: input.wallet_id,
        discovery_rank: metric.as_ref().map(|row| row.rank),
        discovery_score: metric.as_ref().map(|row| row.score),
        discovery_window_start: metric.as_ref().map(|row| row.window_start.to_rfc3339()),
        metric_leader_pnl_sol: metric.as_ref().map(|row| row.pnl_sol),
        metric_win_rate: metric.as_ref().map(|row| row.win_rate),
        metric_closed_trades: metric.as_ref().map(|row| row.closed_trades),
        metric_hold_median_seconds: metric.as_ref().map(|row| row.hold_median_seconds),
        metric_rug_ratio: metric.as_ref().map(|row| row.rug_ratio),
        leader_pnl_sol: leader_pnl,
        leader_trades,
        leader_win_rate: if has_leader_closes {
            rate(
                input.leader_closes.iter().filter(|row| row.win).count() as u64,
                leader_trades,
            )
        } else {
            metric.as_ref().map(|row| row.win_rate)
        },
        leader_hold_median_seconds: metric.as_ref().map(|row| row.hold_median_seconds as f64),
        follower_pnl_sol: follower_pnl,
        follower_trades,
        follower_win_rate: rate(
            input
                .follower_closes
                .iter()
                .filter(|row| row.pnl_sol > 0.0)
                .count() as u64,
            follower_trades,
        ),
        follower_gap_sol: follower_gap,
        copyability_ratio,
        close_context_split: split,
        eligible_for_correlation: leader_trades >= min_leader_trades
            && follower_trades >= min_follower_trades
            && metric.is_some(),
    }
}

fn correlation_summary(wallets: &[WalletCopyabilityRow]) -> CorrelationSummary {
    let eligible = wallets
        .iter()
        .filter(|row| row.eligible_for_correlation)
        .collect::<Vec<_>>();
    let rank_leader_pairs = eligible
        .iter()
        .filter_map(|row| Some((row.discovery_rank? as f64, row.leader_pnl_sol)))
        .collect::<Vec<_>>();
    let rank_follower_pairs = eligible
        .iter()
        .filter_map(|row| Some((row.discovery_rank? as f64, row.follower_pnl_sol)))
        .collect::<Vec<_>>();
    let leader = spearman(rank_leader_pairs);
    let follower = spearman(rank_follower_pairs);
    CorrelationSummary {
        eligible_wallets: eligible.len() as u64,
        rank_vs_leader_pnl_spearman: leader,
        rank_vs_follower_pnl_spearman: follower,
        rank_sign_note:
            "Lower discovery_rank is better; negative rho means top ranks align with higher PnL."
                .to_string(),
        verdict: verdict(eligible.len(), leader, follower),
    }
}

fn close_context_split(rows: &[FollowerCloseFact]) -> CloseContextSplit {
    let mut buckets: HashMap<&'static str, CloseContextBucket> = HashMap::new();
    for key in [
        "market",
        "stale_quote_price",
        "stale_market_price",
        "terminal",
        "other",
    ] {
        buckets.insert(
            key,
            CloseContextBucket {
                count: 0,
                pnl_sol: 0.0,
            },
        );
    }
    for row in rows {
        let key = match row.close_context.as_str() {
            "market" => "market",
            "stale_quote_price" => "stale_quote_price",
            "stale_market_price" => "stale_market_price",
            "stale_terminal_zero_price" | "recovery_terminal_zero_price" => "terminal",
            _ => "other",
        };
        let bucket = buckets.get_mut(key).expect("bucket key must exist");
        bucket.count += 1;
        bucket.pnl_sol += row.pnl_sol;
    }
    CloseContextSplit {
        market: buckets.remove("market").expect("market bucket"),
        stale_quote_price: buckets
            .remove("stale_quote_price")
            .expect("stale quote bucket"),
        stale_market_price: buckets
            .remove("stale_market_price")
            .expect("stale market bucket"),
        terminal: buckets.remove("terminal").expect("terminal bucket"),
        other: buckets.remove("other").expect("other bucket"),
    }
}

fn spearman(mut pairs: Vec<(f64, f64)>) -> Option<f64> {
    pairs.retain(|(left, right)| left.is_finite() && right.is_finite());
    if pairs.len() < 3 {
        return None;
    }
    let left_ranks = ranks(pairs.iter().map(|(left, _)| *left).collect());
    let right_ranks = ranks(pairs.iter().map(|(_, right)| *right).collect());
    pearson(&left_ranks, &right_ranks)
}

fn ranks(values: Vec<f64>) -> Vec<f64> {
    let mut indexed = values
        .into_iter()
        .enumerate()
        .filter(|(_, value)| value.is_finite())
        .collect::<Vec<_>>();
    indexed.sort_by(|(_, left), (_, right)| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let mut ranks = vec![0.0; indexed.len()];
    let mut pos = 0;
    while pos < indexed.len() {
        let start = pos;
        let value = indexed[pos].1;
        while pos + 1 < indexed.len() && indexed[pos + 1].1 == value {
            pos += 1;
        }
        let avg_rank = (start + pos + 2) as f64 / 2.0;
        for idx in start..=pos {
            ranks[indexed[idx].0] = avg_rank;
        }
        pos += 1;
    }
    ranks
}

fn pearson(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 3 {
        return None;
    }
    let n = left.len() as f64;
    let left_mean = left.iter().sum::<f64>() / n;
    let right_mean = right.iter().sum::<f64>() / n;
    let mut cov = 0.0;
    let mut left_var = 0.0;
    let mut right_var = 0.0;
    for (left, right) in left.iter().zip(right) {
        let left_delta = left - left_mean;
        let right_delta = right - right_mean;
        cov += left_delta * right_delta;
        left_var += left_delta * left_delta;
        right_var += right_delta * right_delta;
    }
    (left_var > 0.0 && right_var > 0.0).then_some(cov / left_var.sqrt() / right_var.sqrt())
}

fn verdict(eligible_wallets: usize, leader: Option<f64>, follower: Option<f64>) -> String {
    if eligible_wallets < MIN_STRONG_VERDICT_WALLETS {
        return "underpowered: treat correlations and candidate lists as directional only"
            .to_string();
    }
    match (leader, follower) {
        (Some(leader), Some(follower)) if leader <= -0.4 && follower > -0.2 => {
            "rank preserves leader-PnL input better than follower-PnL outcome; selection objective likely needs copyability".to_string()
        }
        (Some(_), Some(follower)) if follower <= -0.4 => {
            "rank also predicts follower PnL in this window".to_string()
        }
        (Some(_), Some(_)) => "rank relationship is weak or inconclusive".to_string(),
        _ => "not enough eligible wallets for rank correlation".to_string(),
    }
}

fn compare_rows(left: &WalletCopyabilityRow, right: &WalletCopyabilityRow) -> Ordering {
    left.discovery_rank
        .unwrap_or(u64::MAX)
        .cmp(&right.discovery_rank.unwrap_or(u64::MAX))
        .then_with(|| left.wallet_id.cmp(&right.wallet_id))
}

fn percentile(mut values: Vec<f64>, q: f64) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let idx = ((values.len() - 1) as f64 * q).round() as usize;
    values.get(idx).copied()
}

fn rate(count: u64, total: u64) -> Option<f64> {
    (total > 0).then_some(count as f64 / total as f64)
}

fn caveats() -> Vec<String> {
    vec![
        "metric_basis=shadow_follower_paper_relative_only; follower PnL is paper shadow and overstates executable, but is useful for relative wallet ranking until Track-B executable entry data matures.".to_string(),
        "Leader PnL is own-wallet realized PnL from close facts or wallet_metrics fallback; good-for-itself, not automatically good-to-copy.".to_string(),
        "Per-wallet aggregation is directional; leader trades and follower trades are not strict one-to-one matched in this first pass.".to_string(),
        "Selection bias: this report ranks active followed wallets only; it cannot discover copyable wallets outside the followed set.".to_string(),
        "rank_vs_leader_pnl is partly tautological because Discovery score already includes leader realized PnL; interpret it as preservation through saturation/gates, while rank_vs_follower_pnl is the empirical copyability signal.".to_string(),
        "Per-wallet rows are capped by per_wallet_limit; a wallet at the cap should be treated as a lower-bound aggregate until rerun with a higher bound.".to_string(),
    ]
}
