use crate::universe_de_risk_report_caveats::caveats;
use crate::universe_de_risk_report_db::ObservedSolLegRow;
use crate::universe_de_risk_report_replay::{replay_rows, TokenAcc, WalletSegmentAcc};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
pub(crate) struct SummaryThresholds {
    pub min_wallet_trades: u64,
    pub min_wallet_hold_seconds: i64,
    pub fresh_since: DateTime<Utc>,
    pub top_tokens_limit: usize,
}

#[derive(Debug, Serialize)]
pub struct UniverseDeRiskSummary {
    pub caveats: Vec<String>,
    pub totals: OverallTotals,
    pub concentration: ConcentrationSummary,
    pub by_dex: Vec<DexSummary>,
    pub top_tokens: Vec<TokenSummary>,
    pub verdict: String,
}

#[derive(Debug, Serialize)]
pub struct OverallTotals {
    pub rows_seen: u64,
    pub tokens: u64,
    pub wallets: u64,
    pub round_trips: u64,
    pub leader_pnl_sol: f64,
    pub copyable_wallets: u64,
    pub unmatched_sell_events: u64,
    pub unmatched_sell_proceeds_sol: f64,
}
#[derive(Debug, Serialize)]
pub struct ConcentrationSummary {
    pub eligible_wallets: u64,
    pub median_eligible_wallet_pnl_sol: Option<f64>,
    pub positive_eligible_wallet_pnl_sol: f64,
    pub top_wallet_pnl_sol: Option<f64>,
    pub top_wallet_share_of_positive_pnl: Option<f64>,
    pub ex_top_wallet_pnl_sol: Option<f64>,
    pub top_3_wallets_pnl_sol: f64,
    pub ex_top_3_wallets_pnl_sol: Option<f64>,
    pub copyable_token_count: u64,
    pub top_token_copyable_wallets: Option<u64>,
    pub top_token_copyable_wallet_share: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct DexSummary {
    pub dex: String,
    pub tokens: u64,
    pub swaps: u64,
    pub buys: u64,
    pub sells: u64,
    pub distinct_wallets: u64,
    pub volume_sol: f64,
    pub median_notional_sol: Option<f64>,
    pub survival_proxy_token_rate: Option<f64>,
    pub round_trips: u64,
    pub median_hold_seconds: Option<i64>,
    pub leader_pnl_sol: f64,
    pub win_rate: Option<f64>,
    pub loss_rate: Option<f64>,
    pub unmatched_open_cost_sol: f64,
    pub copyable_wallets: u64,
}

#[derive(Debug, Serialize)]
pub struct TokenSummary {
    pub token_mint: String,
    pub dominant_dex: String,
    pub swaps: u64,
    pub buys: u64,
    pub sells: u64,
    pub distinct_wallets: u64,
    pub volume_sol: f64,
    pub median_notional_sol: Option<f64>,
    pub survival_proxy: bool,
    pub round_trips: u64,
    pub median_hold_seconds: Option<i64>,
    pub leader_pnl_sol: f64,
    pub win_rate: Option<f64>,
    pub loss_rate: Option<f64>,
    pub unmatched_open_cost_sol: f64,
    pub copyable_wallets: u64,
}

struct TokenCopyability {
    counts: HashMap<String, u64>,
    wallets: HashSet<String>,
}

pub(crate) fn summarize_universe(
    rows: Vec<ObservedSolLegRow>,
    thresholds: SummaryThresholds,
) -> UniverseDeRiskSummary {
    let replay = replay_rows(rows);
    let token_copyability = token_copyability(&replay.wallet_tokens, thresholds);
    let token_summaries = token_summaries(
        &replay.tokens,
        &token_copyability.counts,
        thresholds.fresh_since,
    );
    let token_count = token_summaries.len() as u64;
    let by_dex = dex_summaries(
        &token_summaries,
        &replay.wallet_segments,
        &replay.dex_wallets,
        thresholds,
    );
    let copyable_wallets = distinct_copyable_wallets(&replay.wallet_totals, thresholds);
    let concentration = concentration_summary(
        &replay.wallet_totals,
        &token_copyability.counts,
        token_copyability.wallets.len() as u64,
        thresholds,
    );
    let round_trips = token_summaries.iter().map(|row| row.round_trips).sum();
    let leader_pnl_sol = token_summaries.iter().map(|row| row.leader_pnl_sol).sum();
    let mut top_tokens = token_summaries;
    top_tokens.sort_by(compare_token_candidates);
    top_tokens.truncate(thresholds.top_tokens_limit);
    let verdict = verdict(copyable_wallets.len(), &by_dex);

    UniverseDeRiskSummary {
        caveats: caveats(),
        totals: OverallTotals {
            rows_seen: by_dex.iter().map(|row| row.swaps).sum(),
            tokens: token_count,
            wallets: replay.all_wallets.len() as u64,
            round_trips,
            leader_pnl_sol,
            copyable_wallets: copyable_wallets.len() as u64,
            unmatched_sell_events: replay.counters.unmatched_sell_events,
            unmatched_sell_proceeds_sol: replay.counters.unmatched_sell_proceeds_sol,
        },
        concentration,
        by_dex,
        top_tokens,
        verdict,
    }
}

fn token_copyability(
    wallet_tokens: &HashMap<(String, String), WalletSegmentAcc>,
    thresholds: SummaryThresholds,
) -> TokenCopyability {
    let mut counts = HashMap::new();
    let mut wallets = HashSet::new();
    for ((wallet, token), acc) in wallet_tokens {
        if wallet_acc_is_copyable(acc, thresholds) {
            *counts.entry(token.clone()).or_default() += 1;
            wallets.insert(wallet.clone());
        }
    }
    TokenCopyability { counts, wallets }
}

fn token_summaries(
    tokens: &HashMap<String, TokenAcc>,
    copyable_counts: &HashMap<String, u64>,
    fresh_since: DateTime<Utc>,
) -> Vec<TokenSummary> {
    tokens
        .iter()
        .map(|(token_mint, acc)| TokenSummary {
            token_mint: token_mint.clone(),
            dominant_dex: dominant_dex(&acc.dex_counts),
            swaps: acc.swaps,
            buys: acc.buys,
            sells: acc.sells,
            distinct_wallets: acc.wallets.len() as u64,
            volume_sol: acc.volume_sol,
            median_notional_sol: median_f64(acc.notionals.clone()),
            survival_proxy: acc.last_ts.is_some_and(|last| last >= fresh_since),
            round_trips: acc.round_trips,
            median_hold_seconds: median_i64(acc.holds.clone()),
            leader_pnl_sol: acc.pnl_sol,
            win_rate: ratio(acc.wins, acc.round_trips),
            loss_rate: ratio(acc.round_trips.saturating_sub(acc.wins), acc.round_trips),
            unmatched_open_cost_sol: acc.unmatched_open_cost_sol,
            copyable_wallets: *copyable_counts.get(token_mint).unwrap_or(&0),
        })
        .collect()
}

fn dex_summaries(
    tokens: &[TokenSummary],
    wallet_segments: &HashMap<(String, String), WalletSegmentAcc>,
    dex_wallets: &HashMap<String, HashSet<String>>,
    thresholds: SummaryThresholds,
) -> Vec<DexSummary> {
    let mut by_dex: HashMap<String, Vec<&TokenSummary>> = HashMap::new();
    for token in tokens {
        by_dex
            .entry(token.dominant_dex.clone())
            .or_default()
            .push(token);
    }
    let mut out = by_dex
        .into_iter()
        .map(|(dex, rows)| summarize_dex(dex, rows, wallet_segments, dex_wallets, thresholds))
        .collect::<Vec<_>>();
    out.sort_by(|a, b| {
        b.copyable_wallets
            .cmp(&a.copyable_wallets)
            .then(a.dex.cmp(&b.dex))
    });
    out
}

fn summarize_dex(
    dex: String,
    rows: Vec<&TokenSummary>,
    wallet_segments: &HashMap<(String, String), WalletSegmentAcc>,
    dex_wallets: &HashMap<String, HashSet<String>>,
    thresholds: SummaryThresholds,
) -> DexSummary {
    let copyable_wallets = wallet_segments
        .iter()
        .filter(|((_, segment), acc)| segment == &dex && wallet_acc_is_copyable(acc, thresholds))
        .map(|((wallet, _), _)| wallet)
        .collect::<HashSet<_>>()
        .len() as u64;
    let wallets = dex_wallets.get(&dex).map_or(0, HashSet::len) as u64;
    let round_trips = rows.iter().map(|row| row.round_trips).sum::<u64>();
    let wins = rows
        .iter()
        .map(|row| (row.win_rate.unwrap_or(0.0) * row.round_trips as f64).round() as u64)
        .sum::<u64>();
    let holds = rows
        .iter()
        .filter_map(|row| row.median_hold_seconds)
        .collect::<Vec<_>>();
    DexSummary {
        dex,
        tokens: rows.len() as u64,
        swaps: rows.iter().map(|row| row.swaps).sum(),
        buys: rows.iter().map(|row| row.buys).sum(),
        sells: rows.iter().map(|row| row.sells).sum(),
        distinct_wallets: wallets,
        volume_sol: rows.iter().map(|row| row.volume_sol).sum(),
        median_notional_sol: median_f64(
            rows.iter()
                .filter_map(|row| row.median_notional_sol)
                .collect::<Vec<_>>(),
        ),
        survival_proxy_token_rate: ratio(
            rows.iter().filter(|row| row.survival_proxy).count() as u64,
            rows.len() as u64,
        ),
        round_trips,
        median_hold_seconds: median_i64(holds),
        leader_pnl_sol: rows.iter().map(|row| row.leader_pnl_sol).sum(),
        win_rate: ratio(wins, round_trips),
        loss_rate: ratio(round_trips.saturating_sub(wins), round_trips),
        unmatched_open_cost_sol: rows.iter().map(|row| row.unmatched_open_cost_sol).sum(),
        copyable_wallets,
    }
}

fn distinct_copyable_wallets(
    wallet_segments: &HashMap<String, WalletSegmentAcc>,
    thresholds: SummaryThresholds,
) -> HashSet<String> {
    wallet_segments
        .iter()
        .filter(|(_, acc)| wallet_acc_is_copyable(acc, thresholds))
        .map(|(wallet, _)| wallet.clone())
        .collect()
}

fn concentration_summary(
    wallet_totals: &HashMap<String, WalletSegmentAcc>,
    token_copyable_counts: &HashMap<String, u64>,
    token_copyable_wallet_count: u64,
    thresholds: SummaryThresholds,
) -> ConcentrationSummary {
    let mut eligible_pnls = wallet_totals
        .values()
        .filter(|acc| wallet_acc_is_eligible(acc, thresholds))
        .map(|acc| acc.pnl_sol)
        .collect::<Vec<_>>();
    eligible_pnls.sort_by(|a, b| b.total_cmp(a));
    let positive_pnl = eligible_pnls
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .sum::<f64>();
    let top_wallet_pnl = eligible_pnls.first().copied();
    let top_wallet_share = match (top_wallet_pnl, positive_pnl > 0.0) {
        (Some(top), true) if top > 0.0 => Some(top / positive_pnl),
        _ => None,
    };
    let top_3_pnl = eligible_pnls.iter().take(3).sum::<f64>();
    let token_counts = token_copyable_counts
        .values()
        .copied()
        .filter(|count| *count > 0)
        .collect::<Vec<_>>();
    let top_token_copyable_wallets = token_counts.iter().copied().max();
    ConcentrationSummary {
        eligible_wallets: eligible_pnls.len() as u64,
        median_eligible_wallet_pnl_sol: median_f64(eligible_pnls.clone()),
        positive_eligible_wallet_pnl_sol: positive_pnl,
        top_wallet_pnl_sol: top_wallet_pnl,
        top_wallet_share_of_positive_pnl: top_wallet_share,
        ex_top_wallet_pnl_sol: top_wallet_pnl.map(|top| eligible_pnls.iter().sum::<f64>() - top),
        top_3_wallets_pnl_sol: top_3_pnl,
        ex_top_3_wallets_pnl_sol: (!eligible_pnls.is_empty())
            .then(|| eligible_pnls.iter().sum::<f64>() - top_3_pnl),
        copyable_token_count: token_counts.len() as u64,
        top_token_copyable_wallets,
        top_token_copyable_wallet_share: top_token_copyable_wallets.and_then(|top| {
            (token_copyable_wallet_count > 0)
                .then_some(top as f64 / token_copyable_wallet_count as f64)
        }),
    }
}

fn wallet_acc_is_eligible(acc: &WalletSegmentAcc, thresholds: SummaryThresholds) -> bool {
    acc.round_trips >= thresholds.min_wallet_trades
        && median_i64(acc.holds.clone()).unwrap_or(0) >= thresholds.min_wallet_hold_seconds
}

fn wallet_acc_is_copyable(acc: &WalletSegmentAcc, thresholds: SummaryThresholds) -> bool {
    wallet_acc_is_eligible(acc, thresholds) && acc.pnl_sol > 0.0
}

fn dominant_dex(counts: &HashMap<String, u64>) -> String {
    counts
        .iter()
        .max_by(|(dex_a, count_a), (dex_b, count_b)| count_a.cmp(count_b).then(dex_b.cmp(dex_a)))
        .map(|(dex, _)| dex.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn compare_token_candidates(a: &TokenSummary, b: &TokenSummary) -> std::cmp::Ordering {
    b.copyable_wallets
        .cmp(&a.copyable_wallets)
        .then_with(|| b.round_trips.cmp(&a.round_trips))
        .then_with(|| b.volume_sol.total_cmp(&a.volume_sol))
        .then_with(|| a.token_mint.cmp(&b.token_mint))
}

fn verdict(copyable_wallet_count: usize, by_dex: &[DexSummary]) -> String {
    if copyable_wallet_count >= 20 {
        "go on SOL-leg slice only: healthy slow-survivable leader population exists; still check concentration and USDC/token-token blind spots"
            .to_string()
    } else if copyable_wallet_count >= 5 {
        "directional on SOL-leg slice only: some slow-survivable leaders exist, but sample is thin and USDC/token-token pairs are excluded".to_string()
    } else if by_dex.iter().any(|row| row.round_trips > 0) {
        "no-go/underpowered on SOL-leg slice only: leader round trips exist, but copyable slow population is tiny; this does not rule out USDC/token-token established markets"
            .to_string()
    } else {
        "no-go on SOL-leg slice only: no usable leader round trips in bounded window; this does not rule out USDC/token-token established markets".to_string()
    }
}

fn ratio(num: u64, denom: u64) -> Option<f64> {
    (denom > 0).then_some(num as f64 / denom as f64)
}

fn median_f64(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    values.sort_by(|a, b| a.total_cmp(b));
    if values.is_empty() {
        None
    } else {
        let mid = values.len() / 2;
        if values.len() % 2 == 0 {
            Some((values[mid - 1] + values[mid]) / 2.0)
        } else {
            Some(values[mid])
        }
    }
}

fn median_i64(mut values: Vec<i64>) -> Option<i64> {
    values.sort_unstable();
    if values.is_empty() {
        None
    } else if values.len() % 2 == 0 {
        let mid = values.len() / 2;
        Some(values[mid - 1] + (values[mid] - values[mid - 1]) / 2)
    } else {
        Some(values[values.len() / 2])
    }
}
