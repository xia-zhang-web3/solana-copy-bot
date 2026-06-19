use crate::{
    ExecutionExitPolicyBaseline, ExecutionExitPolicyContextSummary, ExecutionExitPolicySimConfig,
    ExecutionExitPolicySimParams, ExecutionExitPolicySimReport, ExecutionExitPolicySummary,
    SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE, SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::params;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone)]
struct ClosedTradeRow {
    token: String,
    close_context: String,
    qty: f64,
    entry_cost_sol: f64,
    exit_value_sol: f64,
    pnl_sol: f64,
    opened_ts: DateTime<Utc>,
    closed_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct TradePoint {
    offset_minutes: i64,
    sample_count: usize,
    estimated_value_sol: Option<f64>,
    value_to_entry_ratio: Option<f64>,
}

#[derive(Default)]
struct PriceSample {
    sample_count: usize,
    median_price_sol: Option<f64>,
}

impl SqliteDiscoveryStore {
    pub fn execution_exit_policy_sim_report(
        &self,
        config: ExecutionExitPolicySimConfig,
    ) -> Result<ExecutionExitPolicySimReport> {
        let trades = self.load_exit_policy_closed_trades(config.since, config.limit)?;
        let points = trades
            .iter()
            .map(|trade| self.trade_points(trade, &config))
            .collect::<Result<Vec<_>>>()?;
        let mut policies = Vec::new();

        for minutes in &config.backstop_minutes {
            policies.push(simulate_backstop(&trades, &points, *minutes));
        }
        for ratio in &config.price_collapse_ratios {
            policies.push(simulate_price_collapse(&trades, &points, *ratio));
        }
        for minutes in &config.activity_idle_minutes {
            policies.push(simulate_activity_collapse(&trades, &points, *minutes));
        }

        Ok(ExecutionExitPolicySimReport {
            since: config.since,
            limit: config.limit,
            params: ExecutionExitPolicySimParams {
                sample_window_minutes: config.sample_window_minutes,
                min_sol_notional: config.min_sol_notional,
                min_samples: config.min_samples,
                max_samples_per_checkpoint: config.max_samples_per_checkpoint,
                checkpoint_step_minutes: config.checkpoint_step_minutes,
                horizon_minutes: config.horizon_minutes,
            },
            baseline: summarize_baseline(&trades),
            policies,
            caveats: vec![
                "Observed prices are not executable quotes; simulated exits are directional."
                    .to_string(),
                "Activity-collapse rows without a price mark signal market silence, not guaranteed fill value."
                    .to_string(),
                "Leader-sell exits already appear as market closes; this simulation targets missing/tail exits."
                    .to_string(),
            ],
        })
    }

    fn load_exit_policy_closed_trades(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ClosedTradeRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT token, close_context, qty, entry_cost_sol, exit_value_sol,
                        pnl_sol, opened_ts, closed_ts
                 FROM shadow_closed_trades INDEXED BY idx_shadow_closed_trades_close_context_closed_ts
                 WHERE close_context IN (?2, ?3, ?4, ?5, ?6, ?7)
                   AND closed_ts >= ?1
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT ?8",
            )
            .context("failed to prepare exit policy trade query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    SHADOW_CLOSE_CONTEXT_MARKET,
                    SHADOW_CLOSE_CONTEXT_STALE_MARKET_PRICE,
                    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
                    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
                    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
                    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                    i64::from(limit.max(1)),
                ],
                read_closed_trade_row,
            )
            .context("failed querying exit policy trades")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading exit policy trades")
    }

    fn trade_points(
        &self,
        trade: &ClosedTradeRow,
        config: &ExecutionExitPolicySimConfig,
    ) -> Result<Vec<TradePoint>> {
        let hold_minutes = ((trade.closed_ts - trade.opened_ts).num_seconds() / 60).max(0);
        let horizon = config.horizon_minutes.min(hold_minutes);
        let step = config.checkpoint_step_minutes.max(1);
        let mut points = Vec::new();
        let mut offset = step;
        while offset <= horizon {
            let target_ts = trade.opened_ts + Duration::minutes(offset);
            let price = self.observed_exit_policy_token_sol_price_at(
                &trade.token,
                target_ts,
                config.sample_window_minutes,
                config.min_sol_notional,
                config.min_samples,
                config.max_samples_per_checkpoint,
            )?;
            let estimated_value_sol = price.median_price_sol.map(|price| price * trade.qty);
            points.push(TradePoint {
                offset_minutes: offset,
                sample_count: price.sample_count,
                estimated_value_sol,
                value_to_entry_ratio: estimated_value_sol
                    .filter(|_| trade.entry_cost_sol > 0.0)
                    .map(|value| value / trade.entry_cost_sol),
            });
            offset += step;
        }
        Ok(points)
    }

    fn observed_exit_policy_token_sol_price_at(
        &self,
        token: &str,
        target_ts: DateTime<Utc>,
        window_minutes: i64,
        min_sol_notional: f64,
        min_samples: usize,
        max_samples: usize,
    ) -> Result<PriceSample> {
        let start_ts = target_ts - Duration::minutes(window_minutes.max(1));
        let mut stmt = self
            .conn
            .prepare(
                "SELECT price_sol
                 FROM (
                    SELECT qty_out / qty_in AS price_sol, qty_out AS sol_notional, ts
                    FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                    WHERE token_in = ?1 AND token_out = ?2
                      AND qty_in > 0 AND qty_out > 0
                      AND ts >= ?3 AND ts <= ?4
                    UNION ALL
                    SELECT qty_in / qty_out AS price_sol, qty_in AS sol_notional, ts
                    FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                    WHERE token_in = ?2 AND token_out = ?1
                      AND qty_in > 0 AND qty_out > 0
                      AND ts >= ?3 AND ts <= ?4
                 )
                 WHERE sol_notional >= ?5
                 ORDER BY ts DESC
                 LIMIT ?6",
            )
            .context("failed to prepare exit policy price query")?;
        let rows = stmt
            .query_map(
                params![
                    token,
                    SOL_MINT,
                    start_ts.to_rfc3339(),
                    target_ts.to_rfc3339(),
                    min_sol_notional,
                    max_samples.max(min_samples).min(200) as i64,
                ],
                |row| row.get::<_, f64>(0),
            )
            .context("failed querying exit policy price samples")?;
        let mut prices = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading exit policy price samples")?
            .into_iter()
            .filter(|value| value.is_finite() && *value > 0.0)
            .collect::<Vec<_>>();
        prices.sort_by(f64::total_cmp);
        Ok(PriceSample {
            sample_count: prices.len(),
            median_price_sol: (prices.len() >= min_samples).then(|| median(&prices)),
        })
    }
}

fn simulate_backstop(
    trades: &[ClosedTradeRow],
    points: &[Vec<TradePoint>],
    minutes: i64,
) -> ExecutionExitPolicySummary {
    summarize_policy(
        &format!("backstop_{minutes}m"),
        trades,
        points,
        |trade_points| {
            trade_points
                .iter()
                .find(|point| point.offset_minutes >= minutes)
        },
    )
}

fn simulate_price_collapse(
    trades: &[ClosedTradeRow],
    points: &[Vec<TradePoint>],
    ratio: f64,
) -> ExecutionExitPolicySummary {
    summarize_policy(
        &format!("price_ratio_lte_{ratio:.2}"),
        trades,
        points,
        |trade_points| {
            trade_points.iter().find(|point| {
                point
                    .value_to_entry_ratio
                    .is_some_and(|value| value <= ratio)
            })
        },
    )
}

fn simulate_activity_collapse(
    trades: &[ClosedTradeRow],
    points: &[Vec<TradePoint>],
    minutes: i64,
) -> ExecutionExitPolicySummary {
    summarize_policy(
        &format!("activity_silent_{minutes}m"),
        trades,
        points,
        |trade_points| {
            trade_points
                .iter()
                .find(|point| point.offset_minutes >= minutes && point.sample_count == 0)
        },
    )
}

fn summarize_policy(
    policy: &str,
    trades: &[ClosedTradeRow],
    points: &[Vec<TradePoint>],
    selector: impl for<'a> Fn(&'a [TradePoint]) -> Option<&'a TradePoint>,
) -> ExecutionExitPolicySummary {
    let mut summary = ExecutionExitPolicySummary {
        policy: policy.to_string(),
        ..Default::default()
    };
    for (trade, trade_points) in trades.iter().zip(points) {
        let Some(point) = selector(trade_points) else {
            continue;
        };
        summary.triggered_trades += 1;
        summary.original_pnl_sol += trade.pnl_sol;
        if is_stale_rug_like(&trade.close_context) {
            summary.stale_rug_like_triggered += 1;
        }
        if trade.close_context == SHADOW_CLOSE_CONTEXT_MARKET {
            summary.market_triggered += 1;
        }
        let Some(value) = point.estimated_value_sol else {
            summary.unpriced_triggers += 1;
            continue;
        };
        let simulated_pnl = value - trade.entry_cost_sol;
        let delta = simulated_pnl - trade.pnl_sol;
        summary.priced_triggers += 1;
        summary.simulated_pnl_sol += simulated_pnl;
        summary.net_delta_sol += delta;
        if trade.pnl_sol < 0.0 && delta > 0.0 {
            summary.loss_saved_sol += delta;
        }
        if trade.pnl_sol > 0.0 {
            summary.winners_cut += 1;
            if delta < 0.0 {
                summary.winner_upside_lost_sol += -delta;
            }
        }
        if is_stale_rug_like(&trade.close_context) {
            summary.stale_rug_like_net_delta_sol += delta;
        }
        if trade.close_context == SHADOW_CLOSE_CONTEXT_MARKET {
            summary.market_net_delta_sol += delta;
        }
    }
    summary
}

fn summarize_baseline(trades: &[ClosedTradeRow]) -> ExecutionExitPolicyBaseline {
    let mut baseline = ExecutionExitPolicyBaseline::default();
    let mut contexts =
        std::collections::BTreeMap::<String, ExecutionExitPolicyContextSummary>::new();
    for trade in trades {
        baseline.trades += 1;
        baseline.entry_cost_sol += trade.entry_cost_sol;
        baseline.exit_value_sol += trade.exit_value_sol;
        baseline.pnl_sol += trade.pnl_sol;
        if trade.close_context == SHADOW_CLOSE_CONTEXT_MARKET {
            baseline.market_trades += 1;
            baseline.market_pnl_sol += trade.pnl_sol;
        } else {
            baseline.non_market_trades += 1;
            baseline.non_market_pnl_sol += trade.pnl_sol;
        }
        if is_stale_rug_like(&trade.close_context) {
            baseline.stale_rug_like_trades += 1;
            baseline.stale_rug_like_pnl_sol += trade.pnl_sol;
        }
        let context = contexts
            .entry(trade.close_context.clone())
            .or_insert_with(|| ExecutionExitPolicyContextSummary {
                close_context: trade.close_context.clone(),
                ..Default::default()
            });
        context.trades += 1;
        context.entry_cost_sol += trade.entry_cost_sol;
        context.pnl_sol += trade.pnl_sol;
    }
    baseline.contexts = contexts.into_values().collect();
    baseline
}

fn is_stale_rug_like(close_context: &str) -> bool {
    matches!(
        close_context,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE
            | SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE
            | SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
    )
}

fn read_closed_trade_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClosedTradeRow> {
    let opened_raw: String = row.get(6)?;
    let closed_raw: String = row.get(7)?;
    Ok(ClosedTradeRow {
        token: row.get(0)?,
        close_context: row.get(1)?,
        qty: row.get(2)?,
        entry_cost_sol: row.get(3)?,
        exit_value_sol: row.get(4)?,
        pnl_sol: row.get(5)?,
        opened_ts: parse_sql_ts(&opened_raw, 6)?,
        closed_ts: parse_sql_ts(&closed_raw, 7)?,
    })
}

fn parse_sql_ts(raw: &str, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) / 2.0
    }
}
