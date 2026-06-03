use crate::{
    execution_canary_quote_pnl_accumulator::summarize_quote_pnl,
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, ExecutionCanaryQuotePnlSummary,
    ExecutionCanaryQuotePnlTrade, SqliteDiscoveryStore, EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED,
    EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED, EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

const SOL_LAMPORTS: f64 = 1_000_000_000.0;
const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_SKIP: &str = "would_skip";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

impl SqliteDiscoveryStore {
    pub fn execution_canary_quote_pnl_summary(
        &self,
        as_of: DateTime<Utc>,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionCanaryQuotePnlSummary> {
        ensure_execution_quote_canary_tables(self)?;
        let shadow_close_breakdown = self.execution_canary_shadow_close_breakdown(since)?;
        let rows = self.execution_canary_quote_pnl_rows(since, limit)?;
        let trades = rows
            .into_iter()
            .map(classify_trade)
            .collect::<Result<Vec<_>>>()?;
        Ok(summarize_quote_pnl(
            as_of,
            since,
            limit,
            trades,
            shadow_close_breakdown,
        ))
    }

    fn execution_canary_quote_pnl_rows(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<QuotePnlRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    closed.id,
                    closed.signal_id,
                    closed.wallet_id,
                    closed.token,
                    closed.qty,
                    closed.entry_cost_sol,
                    closed.exit_value_sol,
                    closed.pnl_sol,
                    closed.opened_ts,
                    closed.closed_ts,
                    buy.event_id,
                    buy.quote_status,
                    buy.request_ts,
                    buy.signal_ts,
                    buy.quote_in_amount_raw,
                    buy.quote_out_amount_raw,
                    buy.quote_price_sol,
                    buy.slippage_bps,
                    buy.price_impact_pct,
                    buy.priority_fee_status,
                    buy.priority_fee_lamports,
                    buy.decision_status,
                    buy.decision_reason,
                    sell.event_id,
                    sell.quote_status,
                    sell.request_ts,
                    sell.signal_ts,
                    sell.quote_in_amount_raw,
                    sell.quote_out_amount_raw,
                    sell.quote_price_sol,
                    sell.slippage_bps,
                    sell.price_impact_pct,
                    sell.priority_fee_status,
                    sell.priority_fee_lamports,
                    sell.decision_status,
                    sell.decision_reason
                 FROM shadow_closed_trades AS closed
                 LEFT JOIN execution_quote_canary_events AS sell
                    ON sell.shadow_closed_trade_id = closed.id
                   AND lower(sell.side) = 'sell'
                 LEFT JOIN execution_quote_canary_events AS buy
                    ON buy.event_id = (
                        SELECT candidate.event_id
                        FROM execution_quote_canary_events AS candidate
                        WHERE lower(candidate.side) = 'buy'
                          AND candidate.wallet_id = closed.wallet_id
                          AND candidate.token = closed.token
                          AND substr(candidate.signal_ts, 1, 19) = substr(closed.opened_ts, 1, 19)
                        ORDER BY candidate.request_ts DESC, candidate.event_id DESC
                        LIMIT 1
                    )
                 WHERE closed.closed_ts >= ?1
                   AND COALESCE(closed.close_context, 'market') = 'market'
                 ORDER BY closed.closed_ts DESC, closed.id DESC
                 LIMIT ?2",
            )
            .context("failed to prepare execution canary quote pnl query")?;
        let rows = stmt
            .query_map(
                params![since.to_rfc3339(), i64::from(limit.max(1))],
                |row| read_quote_pnl_row(row),
            )
            .context("failed querying execution canary quote pnl rows")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution canary quote pnl rows")
    }
}

fn classify_trade(row: QuotePnlRow) -> Result<ExecutionCanaryQuotePnlTrade> {
    let mut trade = base_trade(&row);
    trade.entry_quote_event_id = row.buy.event_id.clone();
    trade.exit_quote_event_id = row.sell.event_id.clone();
    trade.entry_quote_status = row.buy.quote_status.clone();
    trade.exit_quote_status = row.sell.quote_status.clone();
    trade.entry_decision_status = row.buy.decision_status.clone();
    trade.exit_decision_status = row.sell.decision_status.clone();
    trade.buy_slippage_bps = row.buy.slippage_bps;
    trade.sell_slippage_bps = row.sell.slippage_bps;
    trade.buy_price_impact_pct = row.buy.price_impact_pct;
    trade.sell_price_impact_pct = row.sell.price_impact_pct;

    let Some(buy_status) = row.buy.quote_status.as_deref() else {
        return Ok(mark_unknown(trade, "missing_entry_quote"));
    };
    let Some(sell_status) = row.sell.quote_status.as_deref() else {
        return Ok(mark_unknown(trade, "missing_exit_quote"));
    };
    if buy_status != QUOTE_STATUS_OK {
        return Ok(mark_unknown(
            trade,
            &format!("entry_quote_status:{buy_status}"),
        ));
    }
    if sell_status != QUOTE_STATUS_OK {
        return Ok(mark_unknown(
            trade,
            &format!("exit_quote_status:{sell_status}"),
        ));
    }

    match row.buy.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE) => {}
        Some(DECISION_WOULD_SKIP) => {
            if let Some(pnl) = compute_quote_pnl(&row)? {
                trade.skipped_counterfactual_pnl_sol = Some(pnl.quote_adjusted_pnl_sol);
                trade.skipped_counterfactual_pnl_after_priority_fee_sol =
                    Some(pnl.quote_adjusted_pnl_after_priority_fee_sol);
                trade.skipped_counterfactual_after_fee_vs_shadow_delta_sol =
                    Some(pnl.quote_adjusted_pnl_after_priority_fee_sol - row.shadow_pnl_sol);
            }
            return Ok(mark_skipped(trade, entry_reason(&row)));
        }
        Some(status) => return Ok(mark_unknown(trade, &format!("entry_decision:{status}"))),
        None => return Ok(mark_unknown(trade, "entry_decision_missing")),
    }
    match row.sell.decision_status.as_deref() {
        Some(DECISION_WOULD_EXECUTE | DECISION_WOULD_FORCE_EXIT) => {}
        Some(status) => return Ok(mark_unknown(trade, &format!("exit_decision:{status}"))),
        None => return Ok(mark_unknown(trade, "exit_decision_missing")),
    }

    let Some(pnl) = compute_quote_pnl(&row)? else {
        return Ok(mark_unknown(trade, "invalid_quote_amount"));
    };
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_COUNTED.to_string();
    trade.reason = if pnl.scaled_exit_to_entry_qty {
        "ok_scaled_to_entry_qty".to_string()
    } else {
        "ok".to_string()
    };
    trade.quote_adjusted_pnl_sol = Some(pnl.quote_adjusted_pnl_sol);
    trade.quote_adjusted_pnl_after_priority_fee_sol =
        Some(pnl.quote_adjusted_pnl_after_priority_fee_sol);
    trade.quote_vs_shadow_delta_sol = Some(pnl.quote_adjusted_pnl_sol - row.shadow_pnl_sol);
    trade.quote_after_fee_vs_shadow_delta_sol =
        Some(pnl.quote_adjusted_pnl_after_priority_fee_sol - row.shadow_pnl_sol);
    trade.entry_cost_sol = Some(pnl.entry_cost_sol);
    trade.exit_quote_sol = Some(pnl.exit_quote_sol);
    trade.closed_qty_ratio = Some(pnl.closed_qty_ratio);
    trade.priority_fee_lamports_total = Some(pnl.priority_fee_lamports_total);
    Ok(trade)
}

fn compute_quote_pnl(row: &QuotePnlRow) -> Result<Option<ComputedPnl>> {
    let Some(entry_in_raw) = parse_raw_amount(row.buy.quote_in_amount_raw.as_deref())? else {
        return Ok(None);
    };
    let Some(entry_out_raw) = parse_raw_amount(row.buy.quote_out_amount_raw.as_deref())? else {
        return Ok(None);
    };
    let Some(exit_in_raw) = parse_raw_amount(row.sell.quote_in_amount_raw.as_deref())? else {
        return Ok(None);
    };
    let Some(exit_out_raw) = parse_raw_amount(row.sell.quote_out_amount_raw.as_deref())? else {
        return Ok(None);
    };
    if entry_in_raw == 0 || entry_out_raw == 0 || exit_in_raw == 0 {
        return Ok(None);
    }

    let exit_qty_for_pnl = exit_in_raw.min(entry_out_raw);
    let closed_qty_ratio = exit_qty_for_pnl as f64 / entry_out_raw as f64;
    let entry_cost_sol = (entry_in_raw as f64 / SOL_LAMPORTS) * closed_qty_ratio;
    let exit_quote_sol =
        (exit_out_raw as f64 / SOL_LAMPORTS) * (exit_qty_for_pnl as f64 / exit_in_raw as f64);
    let priority_fee_lamports_total =
        row.buy.priority_fee_lamports.unwrap_or(0) + row.sell.priority_fee_lamports.unwrap_or(0);
    let quote_adjusted_pnl_sol = exit_quote_sol - entry_cost_sol;
    let quote_adjusted_pnl_after_priority_fee_sol =
        quote_adjusted_pnl_sol - priority_fee_lamports_total as f64 / SOL_LAMPORTS;
    Ok(Some(ComputedPnl {
        entry_cost_sol,
        exit_quote_sol,
        closed_qty_ratio,
        quote_adjusted_pnl_sol,
        quote_adjusted_pnl_after_priority_fee_sol,
        priority_fee_lamports_total,
        scaled_exit_to_entry_qty: exit_in_raw > entry_out_raw,
    }))
}

fn base_trade(row: &QuotePnlRow) -> ExecutionCanaryQuotePnlTrade {
    ExecutionCanaryQuotePnlTrade {
        shadow_closed_trade_id: row.id,
        signal_id: row.signal_id.clone(),
        wallet_id: row.wallet_id.clone(),
        token: row.token.clone(),
        opened_ts: row.opened_ts,
        closed_ts: row.closed_ts,
        status: EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN.to_string(),
        reason: "unclassified".to_string(),
        shadow_pnl_sol: row.shadow_pnl_sol,
        quote_adjusted_pnl_sol: None,
        quote_adjusted_pnl_after_priority_fee_sol: None,
        quote_vs_shadow_delta_sol: None,
        quote_after_fee_vs_shadow_delta_sol: None,
        skipped_counterfactual_pnl_sol: None,
        skipped_counterfactual_pnl_after_priority_fee_sol: None,
        skipped_counterfactual_after_fee_vs_shadow_delta_sol: None,
        entry_quote_event_id: None,
        exit_quote_event_id: None,
        entry_decision_status: None,
        exit_decision_status: None,
        entry_quote_status: None,
        exit_quote_status: None,
        entry_cost_sol: None,
        exit_quote_sol: None,
        closed_qty_ratio: None,
        buy_slippage_bps: None,
        sell_slippage_bps: None,
        buy_price_impact_pct: None,
        sell_price_impact_pct: None,
        priority_fee_lamports_total: None,
    }
}

fn entry_reason(row: &QuotePnlRow) -> String {
    row.buy
        .decision_reason
        .clone()
        .unwrap_or_else(|| "entry_decision:would_skip".to_string())
}

fn mark_unknown(
    mut trade: ExecutionCanaryQuotePnlTrade,
    reason: &str,
) -> ExecutionCanaryQuotePnlTrade {
    if trade.reason == "unclassified" {
        trade.reason = reason.to_string();
    }
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_UNKNOWN.to_string();
    trade
}

fn mark_skipped(
    mut trade: ExecutionCanaryQuotePnlTrade,
    reason: String,
) -> ExecutionCanaryQuotePnlTrade {
    trade.status = EXECUTION_CANARY_QUOTE_PNL_STATUS_SKIPPED.to_string();
    trade.reason = reason;
    trade
}

fn parse_raw_amount(raw: Option<&str>) -> Result<Option<u128>> {
    raw.map(|value| {
        value
            .parse::<u128>()
            .with_context(|| format!("invalid quote raw amount: {value}"))
    })
    .transpose()
}

fn read_quote_pnl_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<QuotePnlRow> {
    read_quote_pnl_row_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        )
    })
}

fn read_quote_pnl_row_result(row: &rusqlite::Row<'_>) -> Result<QuotePnlRow> {
    let opened_ts_raw: String = row.get(8).context("failed reading opened_ts")?;
    let closed_ts_raw: String = row.get(9).context("failed reading closed_ts")?;
    Ok(QuotePnlRow {
        id: row.get(0).context("failed reading closed id")?,
        signal_id: row.get(1).context("failed reading signal_id")?,
        wallet_id: row.get(2).context("failed reading wallet_id")?,
        token: row.get(3).context("failed reading token")?,
        shadow_pnl_sol: row.get(7).context("failed reading pnl_sol")?,
        opened_ts: parse_rfc3339_utc(&opened_ts_raw, "shadow_closed_trades.opened_ts")?,
        closed_ts: parse_rfc3339_utc(&closed_ts_raw, "shadow_closed_trades.closed_ts")?,
        buy: QuoteEvent::read(row, 10)?,
        sell: QuoteEvent::read(row, 23)?,
    })
}

#[derive(Debug, Clone)]
struct QuotePnlRow {
    id: i64,
    signal_id: String,
    wallet_id: String,
    token: String,
    shadow_pnl_sol: f64,
    opened_ts: DateTime<Utc>,
    closed_ts: DateTime<Utc>,
    buy: QuoteEvent,
    sell: QuoteEvent,
}

#[derive(Debug, Clone)]
struct QuoteEvent {
    event_id: Option<String>,
    quote_status: Option<String>,
    quote_in_amount_raw: Option<String>,
    quote_out_amount_raw: Option<String>,
    slippage_bps: Option<f64>,
    price_impact_pct: Option<f64>,
    priority_fee_lamports: Option<u64>,
    decision_status: Option<String>,
    decision_reason: Option<String>,
}

impl QuoteEvent {
    fn read(row: &rusqlite::Row<'_>, offset: usize) -> Result<Self> {
        let priority_fee_raw: Option<i64> = row
            .get(offset + 10)
            .context("failed reading priority_fee_lamports")?;
        Ok(Self {
            event_id: row.get(offset).context("failed reading event_id")?,
            quote_status: row.get(offset + 1).context("failed reading quote_status")?,
            quote_in_amount_raw: row
                .get(offset + 4)
                .context("failed reading quote_in_amount_raw")?,
            quote_out_amount_raw: row
                .get(offset + 5)
                .context("failed reading quote_out_amount_raw")?,
            slippage_bps: row.get(offset + 7).context("failed reading slippage_bps")?,
            price_impact_pct: row
                .get(offset + 8)
                .context("failed reading price_impact_pct")?,
            priority_fee_lamports: priority_fee_raw
                .map(|raw| {
                    u64::try_from(raw)
                        .with_context(|| format!("invalid priority_fee_lamports: {raw}"))
                })
                .transpose()?,
            decision_status: row
                .get(offset + 11)
                .context("failed reading decision_status")?,
            decision_reason: row
                .get(offset + 12)
                .context("failed reading decision_reason")?,
        })
    }
}

struct ComputedPnl {
    entry_cost_sol: f64,
    exit_quote_sol: f64,
    closed_qty_ratio: f64,
    quote_adjusted_pnl_sol: f64,
    quote_adjusted_pnl_after_priority_fee_sol: f64,
    priority_fee_lamports_total: u64,
    scaled_exit_to_entry_qty: bool,
}
