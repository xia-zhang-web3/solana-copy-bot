use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, schema::column_exists, ExecutionQuoteCanaryEventInsert,
    SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension};

const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

impl SqliteDiscoveryStore {
    pub fn load_execution_quote_canary_event_by_id(
        &self,
        event_id: &str,
    ) -> Result<Option<ExecutionQuoteCanaryEventInsert>> {
        ensure_execution_quote_canary_tables(self)?;
        self.conn
            .query_row(
                &format!(
                    "SELECT
                    event_id,
                    signal_id,
                    shadow_closed_trade_id,
                    wallet_id,
                    token,
                    side,
                    quote_status,
                    request_ts,
                    signal_ts,
                    decision_delay_ms,
                    quote_latency_ms,
                    leader_notional_sol,
                    quote_in_amount_raw,
                    quote_out_amount_raw,
                    {},
                    quote_price_sol,
                    shadow_price_sol,
                    slippage_bps,
                    price_impact_pct,
                    route_plan_json,
                    priority_fee_status,
                    priority_fee_lamports,
                    priority_fee_json,
                    decision_status,
                    decision_reason,
                    error
                 FROM execution_quote_canary_events
                 WHERE event_id = ?1
                 LIMIT 1",
                    quote_response_json_expr(self)?
                ),
                params![event_id],
                quote_canary_event_from_row,
            )
            .optional()
            .context("failed loading execution quote canary event by id")
    }

    pub fn load_latest_execution_quote_canary_entry_event(
        &self,
        signal_id: &str,
    ) -> Result<Option<ExecutionQuoteCanaryEventInsert>> {
        ensure_execution_quote_canary_tables(self)?;
        self.conn
            .query_row(
                &format!(
                    "SELECT
                    event_id,
                    signal_id,
                    shadow_closed_trade_id,
                    wallet_id,
                    token,
                    side,
                    quote_status,
                    request_ts,
                    signal_ts,
                    decision_delay_ms,
                    quote_latency_ms,
                    leader_notional_sol,
                    quote_in_amount_raw,
                    quote_out_amount_raw,
                    {},
                    quote_price_sol,
                    shadow_price_sol,
                    slippage_bps,
                    price_impact_pct,
                    route_plan_json,
                    priority_fee_status,
                    priority_fee_lamports,
                    priority_fee_json,
                    decision_status,
                    decision_reason,
                    error
                 FROM execution_quote_canary_events
                 WHERE signal_id = ?1
                   AND lower(side) = 'buy'
                 ORDER BY request_ts DESC, event_id DESC
                 LIMIT 1",
                    quote_response_json_expr(self)?
                ),
                params![signal_id],
                quote_canary_event_from_row,
            )
            .optional()
            .context("failed loading latest execution quote canary entry event")
    }

    pub fn list_execution_quote_canary_close_submit_retry_event_ids(
        &self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<String>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event.event_id
                 FROM execution_quote_canary_events AS event
                 WHERE lower(event.side) = 'sell'
                   AND event.request_ts >= ?1
                   AND event.signal_id IS NOT NULL
                   AND TRIM(event.signal_id) <> ''
                   AND event.quote_status = ?2
                   AND event.decision_status IN (?3, ?4)
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        JOIN orders AS buy_order
                          ON pos.position_id = 'exec-canary-pos:' || buy_order.order_id
                        WHERE pos.token = event.token
                          AND pos.accounting_bucket = ?5
                          AND pos.state = ?6
                          AND event.request_ts >= buy_order.submit_ts
                   )
                   AND NOT EXISTS (
                        SELECT 1
                        FROM orders
                        WHERE orders.signal_id = event.signal_id
                   )
                 ORDER BY event.request_ts ASC, event.event_id ASC
                 LIMIT ?7",
            )
            .context("failed to prepare execution quote canary close retry query")?;
        let rows = stmt
            .query_map(
                params![
                    since.to_rfc3339(),
                    QUOTE_STATUS_OK,
                    DECISION_WOULD_EXECUTE,
                    DECISION_WOULD_FORCE_EXIT,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                |row| row.get(0),
            )
            .context("failed querying execution quote canary close retry events")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading execution quote canary close retry events")
    }
}

fn quote_canary_event_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionQuoteCanaryEventInsert> {
    read_quote_canary_event_row(row).map_err(|error| {
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

fn read_quote_canary_event_row(row: &rusqlite::Row<'_>) -> Result<ExecutionQuoteCanaryEventInsert> {
    let request_ts_raw: String = row.get(7).context("failed reading request_ts")?;
    let signal_ts_raw: Option<String> = row.get(8).context("failed reading signal_ts")?;
    let priority_fee_lamports = optional_i64_to_u64(
        "execution_quote_canary_events.priority_fee_lamports",
        row.get(21)
            .context("failed reading priority_fee_lamports")?,
    )?;
    Ok(ExecutionQuoteCanaryEventInsert {
        event_id: row.get(0).context("failed reading event_id")?,
        signal_id: row.get(1).context("failed reading signal_id")?,
        shadow_closed_trade_id: row
            .get(2)
            .context("failed reading shadow_closed_trade_id")?,
        wallet_id: row.get(3).context("failed reading wallet_id")?,
        token: row.get(4).context("failed reading token")?,
        side: row.get(5).context("failed reading side")?,
        quote_status: row.get(6).context("failed reading quote_status")?,
        request_ts: parse_rfc3339_utc(&request_ts_raw, "execution_quote_canary_events.request_ts")?,
        signal_ts: signal_ts_raw
            .as_deref()
            .map(|raw| parse_rfc3339_utc(raw, "execution_quote_canary_events.signal_ts"))
            .transpose()?,
        decision_delay_ms: optional_i64_to_u64(
            "execution_quote_canary_events.decision_delay_ms",
            row.get(9).context("failed reading decision_delay_ms")?,
        )?,
        quote_latency_ms: optional_i64_to_u64(
            "execution_quote_canary_events.quote_latency_ms",
            row.get(10).context("failed reading quote_latency_ms")?,
        )?,
        leader_notional_sol: row.get(11).context("failed reading leader_notional_sol")?,
        quote_in_amount_raw: row.get(12).context("failed reading quote_in_amount_raw")?,
        quote_out_amount_raw: row.get(13).context("failed reading quote_out_amount_raw")?,
        quote_response_json: row.get(14).context("failed reading quote_response_json")?,
        quote_price_sol: row.get(15).context("failed reading quote_price_sol")?,
        shadow_price_sol: row.get(16).context("failed reading shadow_price_sol")?,
        slippage_bps: row.get(17).context("failed reading slippage_bps")?,
        price_impact_pct: row.get(18).context("failed reading price_impact_pct")?,
        route_plan_json: row.get(19).context("failed reading route_plan_json")?,
        priority_fee_status: row.get(20).context("failed reading priority_fee_status")?,
        priority_fee_lamports,
        priority_fee_json: row.get(22).context("failed reading priority_fee_json")?,
        decision_status: row.get(23).context("failed reading decision_status")?,
        decision_reason: row.get(24).context("failed reading decision_reason")?,
        error: row.get(25).context("failed reading error")?,
    })
}

fn quote_response_json_expr(store: &SqliteDiscoveryStore) -> Result<String> {
    if column_exists(
        store,
        "execution_quote_canary_events",
        "quote_response_json",
    )? {
        Ok("quote_response_json".to_string())
    } else {
        Ok("NULL AS quote_response_json".to_string())
    }
}

fn optional_i64_to_u64(field: &str, value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|raw| {
            u64::try_from(raw).with_context(|| format!("{field} is negative or invalid: {raw}"))
        })
        .transpose()
}
