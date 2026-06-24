use crate::leader_copyability_report_summary::{
    correlation_summary, CloseContextBucket, CloseContextSplit, CorrelationSummary,
    WalletCopyabilityRow,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct CopyabilityCohortSummary {
    pub cohort: String,
    pub rank_min: Option<u64>,
    pub rank_max: Option<u64>,
    pub wallet_count: u64,
    pub eligible_wallets: u64,
    pub leader_pnl_sol: f64,
    pub follower_pnl_sol: f64,
    pub leader_trades: u64,
    pub follower_trades: u64,
    pub close_context_split: CloseContextSplit,
    pub correlation: CorrelationSummary,
}

pub(crate) fn rank_cohort(rank: Option<u64>) -> &'static str {
    match rank {
        Some(1..=15) => "rank_1_15",
        Some(16..=30) => "rank_16_30",
        Some(_) => "rank_gt_30",
        None => "unranked",
    }
}

pub(crate) fn summarize_rank_cohorts(
    wallets: &[WalletCopyabilityRow],
) -> Vec<CopyabilityCohortSummary> {
    [
        ("rank_1_15", Some(1), Some(15)),
        ("rank_16_30", Some(16), Some(30)),
        ("rank_gt_30", None, None),
        ("unranked", None, None),
    ]
    .into_iter()
    .map(|(cohort, rank_min, rank_max)| summarize_cohort(cohort, rank_min, rank_max, wallets))
    .collect()
}

fn summarize_cohort(
    cohort: &str,
    rank_min: Option<u64>,
    rank_max: Option<u64>,
    wallets: &[WalletCopyabilityRow],
) -> CopyabilityCohortSummary {
    let rows = wallets
        .iter()
        .filter(|row| row.rank_cohort == cohort)
        .collect::<Vec<_>>();
    let mut close_context_split = empty_split();
    let mut leader_pnl_sol = 0.0;
    let mut follower_pnl_sol = 0.0;
    let mut leader_trades = 0_u64;
    let mut follower_trades = 0_u64;
    let mut eligible_wallets = 0_u64;
    for row in &rows {
        leader_pnl_sol += row.leader_pnl_sol;
        follower_pnl_sol += row.follower_pnl_sol;
        leader_trades += row.leader_trades;
        follower_trades += row.follower_trades;
        eligible_wallets += u64::from(row.eligible_for_correlation);
        add_split(&mut close_context_split, &row.close_context_split);
    }
    CopyabilityCohortSummary {
        cohort: cohort.to_string(),
        rank_min,
        rank_max,
        wallet_count: rows.len() as u64,
        eligible_wallets,
        leader_pnl_sol,
        follower_pnl_sol,
        leader_trades,
        follower_trades,
        close_context_split,
        correlation: correlation_summary(&rows.into_iter().cloned().collect::<Vec<_>>()),
    }
}

fn empty_split() -> CloseContextSplit {
    CloseContextSplit {
        market: empty_bucket(),
        stale_quote_price: empty_bucket(),
        stale_market_price: empty_bucket(),
        terminal: empty_bucket(),
        other: empty_bucket(),
    }
}

fn empty_bucket() -> CloseContextBucket {
    CloseContextBucket {
        count: 0,
        pnl_sol: 0.0,
    }
}

fn add_split(target: &mut CloseContextSplit, source: &CloseContextSplit) {
    add_bucket(&mut target.market, &source.market);
    add_bucket(&mut target.stale_quote_price, &source.stale_quote_price);
    add_bucket(&mut target.stale_market_price, &source.stale_market_price);
    add_bucket(&mut target.terminal, &source.terminal);
    add_bucket(&mut target.other, &source.other);
}

fn add_bucket(target: &mut CloseContextBucket, source: &CloseContextBucket) {
    target.count += source.count;
    target.pnl_sol += source.pnl_sol;
}
