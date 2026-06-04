use crate::{
    ExecutionCanaryQuoteBucketSummary, ExecutionCanaryQuotePnlSummary,
    ExecutionCanaryQuotePnlThresholdSummary, ExecutionCanaryQuotePnlTrade,
    ExecutionCanaryQuoteRouteCount, ExecutionCanaryQuoteStatusCount,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN, EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT,
};
use std::collections::{BTreeMap, HashMap};

const THRESHOLDS_BPS: [u64; 4] = [150, 300, 500, 1000];

pub(crate) fn record_quote_pnl_buckets(summary: &mut ExecutionCanaryQuotePnlSummary) {
    summary.threshold_summaries = THRESHOLDS_BPS
        .into_iter()
        .map(|threshold| threshold_summary(&summary.trades, threshold))
        .collect();
    summary.buy_slippage_buckets = bucket_summary(&summary.trades, buy_slippage_bucket);
    summary.entry_decision_delay_buckets = bucket_summary(&summary.trades, entry_delay_bucket);
    summary.buy_leader_notional_buckets = bucket_summary(&summary.trades, buy_notional_bucket);
    summary.route_counts = route_counts(&summary.trades);
    summary.priority_fee_status_counts = priority_fee_status_counts(&summary.trades);
}

fn threshold_summary(
    trades: &[ExecutionCanaryQuotePnlTrade],
    threshold_bps: u64,
) -> ExecutionCanaryQuotePnlThresholdSummary {
    let mut summary = ExecutionCanaryQuotePnlThresholdSummary {
        threshold_bps,
        total_closed_trades: trades.len() as u64,
        shadow_pnl_sol: trades.iter().map(|trade| trade.shadow_pnl_sol).sum(),
        ..ExecutionCanaryQuotePnlThresholdSummary::default()
    };
    for trade in trades {
        let Some(quote_after_fee) = quote_after_fee(trade) else {
            summary.unknown_trades += 1;
            continue;
        };
        let Some(slippage_bps) = trade.buy_slippage_bps.filter(|value| value.is_finite()) else {
            summary.unknown_trades += 1;
            continue;
        };
        if trade.status == EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN {
            summary.unknown_trades += 1;
            continue;
        }
        if slippage_bps <= threshold_bps as f64 {
            summary.counted_trades += 1;
            summary.quote_adjusted_pnl_after_priority_fee_sol += quote_after_fee;
            if quote_after_fee > 0.0 {
                summary.quote_win_count += 1;
            } else if quote_after_fee < 0.0 {
                summary.quote_loss_count += 1;
            }
            if is_force_exit(trade) {
                summary.force_exit_counted_trades += 1;
            }
        } else {
            summary.skipped_trades += 1;
            summary.skipped_shadow_pnl_sol += trade.shadow_pnl_sol;
            summary.skipped_counterfactual_pnl_after_priority_fee_sol += quote_after_fee;
            if is_force_exit(trade) {
                summary.force_exit_skipped_entry_trades += 1;
            }
        }
    }
    summary
}

fn bucket_summary(
    trades: &[ExecutionCanaryQuotePnlTrade],
    bucket_fn: fn(&ExecutionCanaryQuotePnlTrade) -> String,
) -> Vec<ExecutionCanaryQuoteBucketSummary> {
    let mut buckets: BTreeMap<String, BucketAcc> = BTreeMap::new();
    for trade in trades {
        buckets.entry(bucket_fn(trade)).or_default().record(trade);
    }
    buckets
        .into_iter()
        .map(|(bucket, acc)| acc.into_summary(bucket))
        .collect()
}

#[derive(Default)]
struct BucketAcc {
    trades: u64,
    shadow_pnl_sol: f64,
    quote_after_fee_sol: f64,
    buy_slippage_bps: Sample,
    sell_slippage_bps: Sample,
    entry_decision_delay_ms: Sample,
    exit_decision_delay_ms: Sample,
    buy_leader_notional_sol: Sample,
    routes: HashMap<String, u64>,
}

impl BucketAcc {
    fn record(&mut self, trade: &ExecutionCanaryQuotePnlTrade) {
        self.trades += 1;
        self.shadow_pnl_sol += trade.shadow_pnl_sol;
        self.quote_after_fee_sol += quote_after_fee(trade).unwrap_or(0.0);
        self.buy_slippage_bps.record(trade.buy_slippage_bps);
        self.sell_slippage_bps.record(trade.sell_slippage_bps);
        self.entry_decision_delay_ms
            .record(trade.entry_decision_delay_ms.map(|value| value as f64));
        self.exit_decision_delay_ms
            .record(trade.exit_decision_delay_ms.map(|value| value as f64));
        self.buy_leader_notional_sol
            .record(trade.buy_leader_notional_sol);
        for label in &trade.entry_route_labels {
            *self.routes.entry(label.clone()).or_default() += 1;
        }
    }

    fn into_summary(self, bucket: String) -> ExecutionCanaryQuoteBucketSummary {
        let mut routes = self
            .routes
            .into_iter()
            .map(|(label, events)| ExecutionCanaryQuoteRouteCount {
                side: "entry".to_string(),
                label,
                events,
            })
            .collect::<Vec<_>>();
        routes.sort_by(|left, right| {
            right
                .events
                .cmp(&left.events)
                .then(left.label.cmp(&right.label))
        });
        routes.truncate(3);
        ExecutionCanaryQuoteBucketSummary {
            bucket,
            trades: self.trades,
            shadow_pnl_sol: self.shadow_pnl_sol,
            quote_adjusted_pnl_after_priority_fee_sol: self.quote_after_fee_sol,
            buy_slippage_bps_avg: self.buy_slippage_bps.avg(),
            sell_slippage_bps_avg: self.sell_slippage_bps.avg(),
            entry_decision_delay_ms_avg: self.entry_decision_delay_ms.avg(),
            exit_decision_delay_ms_avg: self.exit_decision_delay_ms.avg(),
            buy_leader_notional_sol_avg: self.buy_leader_notional_sol.avg(),
            top_routes: routes,
        }
    }
}

#[derive(Default)]
struct Sample {
    count: u64,
    sum: f64,
}

impl Sample {
    fn record(&mut self, value: Option<f64>) {
        let Some(value) = value.filter(|value| value.is_finite()) else {
            return;
        };
        self.count += 1;
        self.sum += value;
    }

    fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

fn route_counts(trades: &[ExecutionCanaryQuotePnlTrade]) -> Vec<ExecutionCanaryQuoteRouteCount> {
    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    for trade in trades {
        for label in &trade.entry_route_labels {
            *counts
                .entry(("entry".to_string(), label.clone()))
                .or_default() += 1;
        }
        for label in &trade.exit_route_labels {
            *counts
                .entry(("exit".to_string(), label.clone()))
                .or_default() += 1;
        }
    }
    counts
        .into_iter()
        .map(|((side, label), events)| ExecutionCanaryQuoteRouteCount {
            side,
            label,
            events,
        })
        .collect()
}

fn priority_fee_status_counts(
    trades: &[ExecutionCanaryQuotePnlTrade],
) -> Vec<ExecutionCanaryQuoteStatusCount> {
    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    for trade in trades {
        record_status(
            &mut counts,
            "entry",
            trade.entry_priority_fee_status.as_deref(),
        );
        record_status(
            &mut counts,
            "exit",
            trade.exit_priority_fee_status.as_deref(),
        );
    }
    counts
        .into_iter()
        .map(|((side, status), events)| ExecutionCanaryQuoteStatusCount {
            side,
            status,
            events,
        })
        .collect()
}

fn record_status(counts: &mut BTreeMap<(String, String), u64>, side: &str, status: Option<&str>) {
    let status = status
        .filter(|value| !value.is_empty())
        .unwrap_or("missing");
    *counts
        .entry((side.to_string(), status.to_string()))
        .or_default() += 1;
}

fn quote_after_fee(trade: &ExecutionCanaryQuotePnlTrade) -> Option<f64> {
    trade
        .quote_adjusted_pnl_after_priority_fee_sol
        .or(trade.skipped_counterfactual_pnl_after_priority_fee_sol)
}

fn is_force_exit(trade: &ExecutionCanaryQuotePnlTrade) -> bool {
    trade.exit_decision_status.as_deref() == Some(EXECUTION_CANARY_SELL_DECISION_FORCE_EXIT)
}

fn buy_slippage_bucket(trade: &ExecutionCanaryQuotePnlTrade) -> String {
    match trade.buy_slippage_bps {
        Some(value) if value <= 150.0 => "<=150".to_string(),
        Some(value) if value <= 300.0 => "150-300".to_string(),
        Some(value) if value <= 500.0 => "300-500".to_string(),
        Some(value) if value <= 1000.0 => "500-1000".to_string(),
        Some(_) => "1000+".to_string(),
        None => "missing".to_string(),
    }
}

fn entry_delay_bucket(trade: &ExecutionCanaryQuotePnlTrade) -> String {
    match trade.entry_decision_delay_ms {
        Some(value) if value < 2_000 => "<2s".to_string(),
        Some(_) => ">=2s".to_string(),
        None => "missing".to_string(),
    }
}

fn buy_notional_bucket(trade: &ExecutionCanaryQuotePnlTrade) -> String {
    match trade.buy_leader_notional_sol {
        Some(value) if value < 0.5 => "<0.5".to_string(),
        Some(value) if value < 1.0 => "0.5-1".to_string(),
        Some(value) if value < 2.0 => "1-2".to_string(),
        Some(value) if value < 5.0 => "2-5".to_string(),
        Some(value) if value < 10.0 => "5-10".to_string(),
        Some(_) => "10+".to_string(),
        None => "missing".to_string(),
    }
}
