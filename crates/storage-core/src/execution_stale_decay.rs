use crate::{SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::params;
use serde::Serialize;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const DEFAULT_CHECKPOINT_MINUTES: &[i64] = &[30, 60, 120, 240, 360, 720];

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionStaleDecayReport {
    pub since: DateTime<Utc>,
    pub close_context: String,
    pub trades: u64,
    pub window_minutes: i64,
    pub min_sol_notional: f64,
    pub min_samples: usize,
    pub max_samples_per_checkpoint: usize,
    pub summaries: Vec<ExecutionStaleDecayCheckpointSummary>,
    pub rows: Vec<ExecutionStaleDecayTrade>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionStaleDecayTrade {
    pub shadow_closed_trade_id: i64,
    pub wallet_id: String,
    pub token: String,
    pub opened_ts: DateTime<Utc>,
    pub closed_ts: DateTime<Utc>,
    pub hold_seconds: i64,
    pub qty: f64,
    pub entry_cost_sol: f64,
    pub exit_value_sol: f64,
    pub pnl_sol: f64,
    pub points: Vec<ExecutionStaleDecayPoint>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionStaleDecayPoint {
    pub offset_minutes: i64,
    pub target_ts: DateTime<Utc>,
    pub eligible: bool,
    pub sample_count: usize,
    pub median_price_sol: Option<f64>,
    pub estimated_value_sol: Option<f64>,
    pub value_to_entry_ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionStaleDecayCheckpointSummary {
    pub offset_minutes: i64,
    pub eligible_trades: u64,
    pub priced_trades: u64,
    pub missing_price_trades: u64,
    pub avg_value_to_entry_ratio: Option<f64>,
    pub p10_value_to_entry_ratio: Option<f64>,
    pub p50_value_to_entry_ratio: Option<f64>,
    pub p90_value_to_entry_ratio: Option<f64>,
}

struct StaleClosedTradeRow {
    id: i64,
    wallet_id: String,
    token: String,
    qty: f64,
    entry_cost_sol: f64,
    exit_value_sol: f64,
    pnl_sol: f64,
    opened_ts: DateTime<Utc>,
    closed_ts: DateTime<Utc>,
}

impl SqliteDiscoveryStore {
    pub fn execution_stale_quote_decay_report(
        &self,
        since: DateTime<Utc>,
        limit: u32,
        window_minutes: i64,
        min_sol_notional: f64,
        min_samples: usize,
        max_samples_per_checkpoint: usize,
    ) -> Result<ExecutionStaleDecayReport> {
        let rows = self.load_stale_quote_closed_trades(since, limit)?;
        let window_minutes = window_minutes.max(1);
        let min_samples = min_samples.max(1);
        let max_samples = max_samples_per_checkpoint.max(min_samples).min(200);
        let mut trades = Vec::with_capacity(rows.len());

        for row in rows {
            let mut points = Vec::with_capacity(DEFAULT_CHECKPOINT_MINUTES.len());
            for offset_minutes in DEFAULT_CHECKPOINT_MINUTES {
                let target_ts = row.opened_ts + Duration::minutes(*offset_minutes);
                let eligible = target_ts <= row.closed_ts;
                let price = if eligible {
                    self.observed_token_sol_price_at(
                        &row.token,
                        target_ts,
                        window_minutes,
                        min_sol_notional,
                        min_samples,
                        max_samples,
                    )?
                } else {
                    PriceSample::default()
                };
                let estimated_value_sol = price.median_price_sol.map(|price| price * row.qty);
                points.push(ExecutionStaleDecayPoint {
                    offset_minutes: *offset_minutes,
                    target_ts,
                    eligible,
                    sample_count: price.sample_count,
                    median_price_sol: price.median_price_sol,
                    estimated_value_sol,
                    value_to_entry_ratio: estimated_value_sol
                        .filter(|_| row.entry_cost_sol > 0.0)
                        .map(|value| value / row.entry_cost_sol),
                });
            }
            trades.push(ExecutionStaleDecayTrade {
                shadow_closed_trade_id: row.id,
                wallet_id: row.wallet_id,
                token: row.token,
                opened_ts: row.opened_ts,
                closed_ts: row.closed_ts,
                hold_seconds: (row.closed_ts - row.opened_ts).num_seconds(),
                qty: row.qty,
                entry_cost_sol: row.entry_cost_sol,
                exit_value_sol: row.exit_value_sol,
                pnl_sol: row.pnl_sol,
                points,
            });
        }

        Ok(ExecutionStaleDecayReport {
            since,
            close_context: SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE.to_string(),
            trades: trades.len() as u64,
            window_minutes,
            min_sol_notional,
            min_samples,
            max_samples_per_checkpoint: max_samples,
            summaries: summarize_decay_points(&trades),
            rows: trades,
        })
    }

    fn load_stale_quote_closed_trades(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StaleClosedTradeRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol,
                        opened_ts, closed_ts
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') = ?2
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT ?3",
            )
            .context("failed to prepare stale quote decay trade query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
                    i64::from(limit.max(1)),
                ],
                read_stale_closed_trade_row,
            )
            .context("failed querying stale quote decay trades")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading stale quote decay trades")
    }

    fn observed_token_sol_price_at(
        &self,
        token: &str,
        target_ts: DateTime<Utc>,
        window_minutes: i64,
        min_sol_notional: f64,
        min_samples: usize,
        max_samples: usize,
    ) -> Result<PriceSample> {
        let start_ts = target_ts - Duration::minutes(window_minutes);
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
            .context("failed to prepare stale decay observed price query")?;
        let rows = stmt
            .query_map(
                params![
                    token,
                    SOL_MINT,
                    start_ts.to_rfc3339(),
                    target_ts.to_rfc3339(),
                    min_sol_notional,
                    max_samples as i64,
                ],
                |row| row.get::<_, f64>(0),
            )
            .context("failed querying stale decay observed price samples")?;
        let mut prices = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading stale decay observed price samples")?
            .into_iter()
            .filter(|value| value.is_finite() && *value > 0.0)
            .collect::<Vec<_>>();
        prices.sort_by(f64::total_cmp);
        let median_price_sol = (prices.len() >= min_samples).then(|| median(&prices));
        Ok(PriceSample {
            sample_count: prices.len(),
            median_price_sol,
        })
    }
}

#[derive(Default)]
struct PriceSample {
    sample_count: usize,
    median_price_sol: Option<f64>,
}

fn read_stale_closed_trade_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StaleClosedTradeRow> {
    let opened_raw: String = row.get(7)?;
    let closed_raw: String = row.get(8)?;
    Ok(StaleClosedTradeRow {
        id: row.get(0)?,
        wallet_id: row.get(1)?,
        token: row.get(2)?,
        qty: row.get(3)?,
        entry_cost_sol: row.get(4)?,
        exit_value_sol: row.get(5)?,
        pnl_sol: row.get(6)?,
        opened_ts: parse_sql_ts(&opened_raw, 7)?,
        closed_ts: parse_sql_ts(&closed_raw, 8)?,
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

fn summarize_decay_points(
    trades: &[ExecutionStaleDecayTrade],
) -> Vec<ExecutionStaleDecayCheckpointSummary> {
    DEFAULT_CHECKPOINT_MINUTES
        .iter()
        .map(|offset| {
            let points = trades
                .iter()
                .filter_map(|trade| {
                    trade
                        .points
                        .iter()
                        .find(|point| point.offset_minutes == *offset)
                })
                .collect::<Vec<_>>();
            let eligible_trades = points.iter().filter(|point| point.eligible).count() as u64;
            let mut ratios = points
                .iter()
                .filter_map(|point| point.value_to_entry_ratio)
                .collect::<Vec<_>>();
            ratios.sort_by(f64::total_cmp);
            ExecutionStaleDecayCheckpointSummary {
                offset_minutes: *offset,
                eligible_trades,
                priced_trades: ratios.len() as u64,
                missing_price_trades: eligible_trades.saturating_sub(ratios.len() as u64),
                avg_value_to_entry_ratio: avg(&ratios),
                p10_value_to_entry_ratio: percentile(&ratios, 0.10),
                p50_value_to_entry_ratio: percentile(&ratios, 0.50),
                p90_value_to_entry_ratio: percentile(&ratios, 0.90),
            }
        })
        .collect()
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.len() % 2 == 1 {
        sorted[sorted.len() / 2]
    } else {
        let upper = sorted.len() / 2;
        (sorted[upper - 1] + sorted[upper]) / 2.0
    }
}

fn avg(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let max_index = sorted.len().saturating_sub(1);
    let index = ((max_index as f64) * p).round() as usize;
    Some(sorted[index.min(max_index)])
}
