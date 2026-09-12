use crate::execution_canary_quote_pnl_compute::QuotePnlAmounts;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub(crate) struct QuotePnlRow {
    pub(crate) id: i64,
    pub(crate) signal_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) shadow_pnl_sol: f64,
    pub(crate) opened_ts: DateTime<Utc>,
    pub(crate) closed_ts: DateTime<Utc>,
    pub(crate) market: bool,
    pub(crate) allocation: crate::ExecutionQuoteFeeAllocation,
    pub(crate) entry_attributed: bool,
    pub(crate) binding_error: Option<&'static str>,
    pub(crate) buy: QuoteEvent,
    pub(crate) sell: QuoteEvent,
}

impl QuotePnlRow {
    pub(crate) fn amounts(&self) -> QuotePnlAmounts<'_> {
        QuotePnlAmounts {
            entry_in_raw: self.buy.quote_in_amount_raw.as_deref(),
            entry_out_raw: self.buy.quote_out_amount_raw.as_deref(),
            exit_in_raw: self.sell.quote_in_amount_raw.as_deref(),
            exit_out_raw: self.sell.quote_out_amount_raw.as_deref(),
            buy_priority_fee_lamports: self
                .allocation
                .buy_fee_allocated_lamports
                .as_deref()
                .and_then(|v| v.parse().ok()),
            sell_priority_fee_lamports: self.sell.priority_fee_lamports,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QuoteEvent {
    pub(crate) http_request_started_ts: Option<DateTime<Utc>>,
    pub(crate) event_id: Option<String>,
    pub(crate) signal_id: Option<String>,
    pub(crate) closed_id: Option<i64>,
    pub(crate) wallet: String,
    pub(crate) token: String,
    pub(crate) side: String,
    pub(crate) signal_ts: Option<DateTime<Utc>>,
    pub(crate) request_ts: Option<DateTime<Utc>>,
    pub(crate) quote_status: Option<String>,
    pub(crate) decision_delay_ms: Option<u64>,
    pub(crate) quote_latency_ms: Option<u64>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) shadow_price_sol: Option<f64>,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) price_impact_pct: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_status: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) leader_notional_sol: Option<f64>,
    pub(crate) decision_status: Option<String>,
    pub(crate) decision_reason: Option<String>,
    pub(crate) quote_in_amount_raw: Option<String>,
    pub(crate) quote_out_amount_raw: Option<String>,
}

impl QuoteEvent {
    pub(crate) fn read(row: &rusqlite::Row<'_>, offset: usize) -> Result<Self> {
        let priority_fee_raw: Option<i64> = row
            .get(offset + 14)
            .context("failed reading priority_fee_lamports")?;
        let actual = crate::quote_http_timing::read_http_started(row, offset + 23)?;
        Ok(Self {
            http_request_started_ts: actual,
            event_id: row.get(offset).context("failed reading event_id")?,
            signal_id: row.get(offset + 18)?,
            closed_id: row.get(offset + 19)?,
            wallet: row.get(offset + 20)?,
            token: row.get(offset + 21)?,
            side: row.get(offset + 22)?,
            signal_ts: read_time(row, offset + 3)?,
            request_ts: read_time(row, offset + 2)?,
            quote_status: row.get(offset + 1).context("failed reading quote_status")?,
            decision_delay_ms: crate::quote_http_timing::actual_delay_ms(
                read_time(row, offset + 3)?,
                actual,
            ),
            quote_latency_ms: optional_i64_to_u64(
                "execution_quote_canary_events.quote_latency_ms",
                row.get(offset + 5)
                    .context("failed reading quote_latency_ms")?,
            )?
            .filter(|_| actual.is_some()),
            quote_in_amount_raw: row
                .get(offset + 6)
                .context("failed reading quote_in_amount_raw")?,
            quote_out_amount_raw: row
                .get(offset + 7)
                .context("failed reading quote_out_amount_raw")?,
            quote_price_sol: row
                .get(offset + 8)
                .context("failed reading quote_price_sol")?,
            shadow_price_sol: row
                .get(offset + 9)
                .context("failed reading shadow_price_sol")?,
            slippage_bps: row
                .get(offset + 10)
                .context("failed reading slippage_bps")?,
            price_impact_pct: row
                .get(offset + 11)
                .context("failed reading price_impact_pct")?,
            route_plan_json: row
                .get(offset + 12)
                .context("failed reading route_plan_json")?,
            priority_fee_status: row
                .get(offset + 13)
                .context("failed reading priority_fee_status")?,
            priority_fee_lamports: priority_fee_raw.and_then(|raw| u64::try_from(raw).ok()),
            decision_status: row
                .get(offset + 15)
                .context("failed reading decision_status")?,
            decision_reason: row
                .get(offset + 16)
                .context("failed reading decision_reason")?,
            leader_notional_sol: row
                .get(offset + 17)
                .context("failed reading leader_notional_sol")?,
        })
    }
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).with_context(|| format!("{field} is negative or invalid: {raw}"))
        })
        .transpose()
}

fn read_time(row: &rusqlite::Row<'_>, offset: usize) -> Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = row.get(offset)?;
    Ok(raw.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|t| t.with_timezone(&Utc))
    }))
}
