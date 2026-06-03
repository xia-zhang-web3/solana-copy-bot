use crate::{
    execution_canary_quote_pnl_compute::QuotePnlAmounts, observed_timestamp::parse_rfc3339_utc,
};
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
            buy_priority_fee_lamports: self.buy.priority_fee_lamports,
            sell_priority_fee_lamports: self.sell.priority_fee_lamports,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QuoteEvent {
    pub(crate) event_id: Option<String>,
    pub(crate) quote_status: Option<String>,
    pub(crate) decision_delay_ms: Option<u64>,
    pub(crate) quote_latency_ms: Option<u64>,
    pub(crate) quote_price_sol: Option<f64>,
    pub(crate) shadow_price_sol: Option<f64>,
    pub(crate) slippage_bps: Option<f64>,
    pub(crate) price_impact_pct: Option<f64>,
    pub(crate) route_plan_json: Option<String>,
    pub(crate) priority_fee_lamports: Option<u64>,
    pub(crate) decision_status: Option<String>,
    pub(crate) decision_reason: Option<String>,
    quote_in_amount_raw: Option<String>,
    quote_out_amount_raw: Option<String>,
}

pub(crate) fn read_quote_pnl_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<QuotePnlRow> {
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
        sell: QuoteEvent::read(row, 27)?,
    })
}

impl QuoteEvent {
    fn read(row: &rusqlite::Row<'_>, offset: usize) -> Result<Self> {
        let priority_fee_raw: Option<i64> = row
            .get(offset + 14)
            .context("failed reading priority_fee_lamports")?;
        Ok(Self {
            event_id: row.get(offset).context("failed reading event_id")?,
            quote_status: row.get(offset + 1).context("failed reading quote_status")?,
            decision_delay_ms: optional_i64_to_u64(
                "execution_quote_canary_events.decision_delay_ms",
                row.get(offset + 4)
                    .context("failed reading decision_delay_ms")?,
            )?,
            quote_latency_ms: optional_i64_to_u64(
                "execution_quote_canary_events.quote_latency_ms",
                row.get(offset + 5)
                    .context("failed reading quote_latency_ms")?,
            )?,
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
            priority_fee_lamports: priority_fee_raw
                .map(|raw| {
                    u64::try_from(raw)
                        .with_context(|| format!("invalid priority_fee_lamports: {raw}"))
                })
                .transpose()?,
            decision_status: row
                .get(offset + 15)
                .context("failed reading decision_status")?,
            decision_reason: row
                .get(offset + 16)
                .context("failed reading decision_reason")?,
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
