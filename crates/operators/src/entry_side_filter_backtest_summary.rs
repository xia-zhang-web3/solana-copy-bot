use crate::entry_side_filter_backtest_db::ClosedTrade;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub(crate) struct EnrichedTrade {
    pub(crate) trade: ClosedTrade,
    pub(crate) wallet_history_total: u64,
    pub(crate) wallet_history_rug: u64,
    pub(crate) wallet_history_tail: u64,
    pub(crate) token_prior_bad: bool,
    pub(crate) token_age_seconds: Option<i64>,
    pub(crate) leader_entry_lag_seconds: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct OutcomeSummary {
    pub event_count: u64,
    pub pnl_sol: f64,
    pub entry_cost_sol: f64,
    pub avg_pnl_sol: Option<f64>,
    pub median_pnl_sol: Option<f64>,
    pub winner_events: u64,
    pub loser_events: u64,
    pub rug_like_events: u64,
    pub stale_market_events: u64,
    pub market_events: u64,
}

#[derive(Debug, Serialize)]
pub struct GateReport {
    pub gate: String,
    pub threshold: String,
    pub rejected: OutcomeSummary,
    pub kept: OutcomeSummary,
    pub shadow_delta_if_rejected_sol: f64,
    pub rejection_rate: f64,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct DataQuality {
    pub evaluated_events: u64,
    pub insufficient_wallet_history_events: u64,
    pub missing_token_age_events: u64,
    pub missing_leader_lag_events: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SummaryParams {
    pub min_wallet_history_closes: u64,
    pub tail_loss_sol: f64,
    pub wallet_rug_rate_thresholds: Vec<f64>,
    pub wallet_tail_rate_thresholds: Vec<f64>,
    pub token_age_minutes: Vec<i64>,
    pub leader_lag_seconds: Vec<i64>,
}

#[derive(Debug, Serialize)]
pub struct BacktestSummary {
    pub metric_basis: String,
    pub point_in_time: bool,
    pub baseline: OutcomeSummary,
    pub data_quality: DataQuality,
    pub gates: Vec<GateReport>,
}

pub(crate) fn enrich_trade(
    trade: ClosedTrade,
    history: &[ClosedTrade],
    token_first_seen: Option<DateTime<Utc>>,
    leader_buy_ts: Option<DateTime<Utc>>,
    history_window: Duration,
    tail_loss_sol: f64,
) -> EnrichedTrade {
    let history_cutoff = trade.opened_ts - history_window;
    let wallet_history = history.iter().filter(|row| {
        row.wallet_id == trade.wallet_id
            && row.closed_ts < trade.opened_ts
            && row.closed_ts >= history_cutoff
    });
    let mut wallet_history_total = 0;
    let mut wallet_history_rug = 0;
    let mut wallet_history_tail = 0;
    for row in wallet_history {
        wallet_history_total += 1;
        if is_rug_like(&row.close_context) {
            wallet_history_rug += 1;
        }
        if row.pnl_sol <= tail_loss_sol {
            wallet_history_tail += 1;
        }
    }
    let token_prior_bad = history.iter().any(|row| {
        row.token == trade.token
            && row.closed_ts < trade.opened_ts
            && is_rug_like(&row.close_context)
    });
    let token_age_seconds = token_first_seen.map(|ts| (trade.opened_ts - ts).num_seconds());
    let leader_entry_lag_seconds = leader_buy_ts.map(|ts| (trade.opened_ts - ts).num_seconds());
    EnrichedTrade {
        trade,
        wallet_history_total,
        wallet_history_rug,
        wallet_history_tail,
        token_prior_bad,
        token_age_seconds,
        leader_entry_lag_seconds,
    }
}

pub(crate) fn summarize_entry_side_filters(
    trades: &[EnrichedTrade],
    params: &SummaryParams,
) -> BacktestSummary {
    let baseline = summarize_outcomes(trades.iter().map(|row| &row.trade));
    let gates = build_gate_reports(trades, params);
    BacktestSummary {
        metric_basis: "shadow_outcome_not_executable".to_string(),
        point_in_time: true,
        baseline,
        data_quality: data_quality(trades, params.min_wallet_history_closes),
        gates,
    }
}

fn build_gate_reports(trades: &[EnrichedTrade], params: &SummaryParams) -> Vec<GateReport> {
    let mut reports = Vec::new();
    for threshold in &params.wallet_rug_rate_thresholds {
        reports.push(gate_report(
            trades,
            "wallet_rug_rate",
            &format!(
                "rate>={threshold:.2} min_closes={}",
                params.min_wallet_history_closes
            ),
            |row| {
                wallet_rate(row.wallet_history_rug, row.wallet_history_total, params)
                    >= Some(*threshold)
            },
            "Uses only prior wallet closes; stale_market is not counted as rug-like.",
        ));
    }
    for threshold in &params.wallet_tail_rate_thresholds {
        reports.push(gate_report(
            trades,
            "wallet_tail_rate",
            &format!("rate>={threshold:.2} pnl<={:.3}", params.tail_loss_sol),
            |row| {
                wallet_rate(row.wallet_history_tail, row.wallet_history_total, params)
                    >= Some(*threshold)
            },
            "Uses prior wallet closes with pnl below the configured tail-loss threshold.",
        ));
    }
    reports.push(gate_report(
        trades,
        "token_seen_before_bad",
        "any_prior_rug_like_close",
        |row| row.token_prior_bad,
        "Rejects a token if any earlier closed trade for the token was rug-like.",
    ));
    for minutes in &params.token_age_minutes {
        reports.push(gate_report(
            trades,
            "token_age",
            &format!("age<{}m", minutes),
            |row| {
                row.token_age_seconds
                    .map(|age| age < minutes * 60)
                    .unwrap_or(false)
            },
            "Uses first observed swap for the token before entry; missing age does not reject.",
        ));
    }
    for seconds in &params.leader_lag_seconds {
        reports.push(gate_report(
            trades,
            "leader_entry_lag",
            &format!("lag>{seconds}s"),
            |row| {
                row.leader_entry_lag_seconds
                    .map(|lag| lag > *seconds)
                    .unwrap_or(false)
            },
            "Uses latest observed leader buy before shadow entry; missing lag does not reject.",
        ));
    }
    reports.push(gate_report(
        trades,
        "combined_conservative",
        "wallet_rug>=0.20 OR token_seen_before_bad",
        |row| {
            wallet_rate(row.wallet_history_rug, row.wallet_history_total, params) >= Some(0.20)
                || row.token_prior_bad
        },
        "Fixed non-tuned combo for floor-safety review; do not treat as optimized.",
    ));
    reports
}

fn gate_report<F>(
    trades: &[EnrichedTrade],
    gate: &str,
    threshold: &str,
    predicate: F,
    note: &str,
) -> GateReport
where
    F: Fn(&EnrichedTrade) -> bool,
{
    let rejected = trades
        .iter()
        .filter(|row| predicate(row))
        .collect::<Vec<_>>();
    let kept = trades
        .iter()
        .filter(|row| !predicate(row))
        .collect::<Vec<_>>();
    let rejected_summary = summarize_outcomes(rejected.iter().map(|row| &row.trade));
    let kept_summary = summarize_outcomes(kept.iter().map(|row| &row.trade));
    let rejection_rate = if trades.is_empty() {
        0.0
    } else {
        rejected.len() as f64 / trades.len() as f64
    };
    GateReport {
        gate: gate.to_string(),
        threshold: threshold.to_string(),
        shadow_delta_if_rejected_sol: -rejected_summary.pnl_sol,
        rejected: rejected_summary,
        kept: kept_summary,
        rejection_rate,
        note: note.to_string(),
    }
}

fn summarize_outcomes<'a>(trades: impl Iterator<Item = &'a ClosedTrade>) -> OutcomeSummary {
    let rows = trades.collect::<Vec<_>>();
    let pnl_values = rows.iter().map(|row| row.pnl_sol).collect::<Vec<_>>();
    let pnl_sol = pnl_values.iter().sum::<f64>();
    let event_count = rows.len() as u64;
    OutcomeSummary {
        event_count,
        pnl_sol,
        entry_cost_sol: rows.iter().map(|row| row.entry_cost_sol).sum(),
        avg_pnl_sol: (event_count > 0).then_some(pnl_sol / event_count as f64),
        median_pnl_sol: median(pnl_values),
        winner_events: rows.iter().filter(|row| row.pnl_sol > 0.0).count() as u64,
        loser_events: rows.iter().filter(|row| row.pnl_sol < 0.0).count() as u64,
        rug_like_events: rows
            .iter()
            .filter(|row| is_rug_like(&row.close_context))
            .count() as u64,
        stale_market_events: rows
            .iter()
            .filter(|row| row.close_context == "stale_market_price")
            .count() as u64,
        market_events: rows
            .iter()
            .filter(|row| row.close_context == "market")
            .count() as u64,
    }
}

fn data_quality(trades: &[EnrichedTrade], min_wallet_history_closes: u64) -> DataQuality {
    DataQuality {
        evaluated_events: trades.len() as u64,
        insufficient_wallet_history_events: trades
            .iter()
            .filter(|row| row.wallet_history_total < min_wallet_history_closes)
            .count() as u64,
        missing_token_age_events: trades
            .iter()
            .filter(|row| row.token_age_seconds.is_none())
            .count() as u64,
        missing_leader_lag_events: trades
            .iter()
            .filter(|row| row.leader_entry_lag_seconds.is_none())
            .count() as u64,
    }
}

fn wallet_rate(count: u64, total: u64, params: &SummaryParams) -> Option<f64> {
    (total >= params.min_wallet_history_closes).then_some(count as f64 / total as f64)
}

fn is_rug_like(close_context: &str) -> bool {
    matches!(
        close_context,
        "stale_quote_price" | "stale_terminal_zero_price" | "recovery_terminal_zero_price"
    )
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}
