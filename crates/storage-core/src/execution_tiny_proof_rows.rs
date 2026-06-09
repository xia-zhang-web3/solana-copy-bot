use crate::{
    observed_timestamp::parse_rfc3339_utc, schema::column_exists, ExecutionTinyProofOpenPosition,
    ExecutionTinyProofOrder, SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

#[derive(Debug)]
pub(crate) struct ProofRow {
    pub(crate) shadow_closed_trade_id: i64,
    pub(crate) signal_id: String,
    pub(crate) wallet_id: String,
    pub(crate) token: String,
    pub(crate) shadow_pnl_sol: f64,
    pub(crate) opened_ts: DateTime<Utc>,
    pub(crate) closed_ts: DateTime<Utc>,
    pub(crate) buy_quote: QuoteSide,
    pub(crate) sell_quote: QuoteSide,
    pub(crate) buy_order: Option<ExecutionTinyProofOrder>,
    pub(crate) sell_order: Option<ExecutionTinyProofOrder>,
    pub(crate) position: PositionSide,
}

#[derive(Debug, Default)]
pub(crate) struct QuoteSide {
    pub(crate) event_id: Option<String>,
    pub(crate) quote_status: Option<String>,
    pub(crate) decision_status: Option<String>,
    pub(crate) decision_reason: Option<String>,
    request_ts: Option<DateTime<Utc>>,
    signal_ts: Option<DateTime<Utc>>,
    pub(crate) quote_latency_ms: Option<u64>,
    pub(crate) decision_delay_ms: Option<u64>,
    pub(crate) priority_fee_lamports: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct PositionSide {
    pub(crate) state: Option<String>,
    pub(crate) opened_ts: Option<DateTime<Utc>>,
    pub(crate) closed_ts: Option<DateTime<Utc>>,
    pub(crate) cost_sol: Option<f64>,
    pub(crate) pnl_sol: Option<f64>,
}

pub(crate) fn execution_tiny_proof_rows(
    store: &SqliteDiscoveryStore,
    since: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ProofRow>> {
    let mut stmt = store
        .conn
        .prepare(PROOF_ROWS_SQL)
        .context("failed to prepare execution tiny proof query")?;
    let rows = stmt
        .query_map(
            params![since.to_rfc3339(), i64::from(limit.max(1))],
            read_proof_row,
        )
        .context("failed querying execution tiny proof rows")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading execution tiny proof rows")
}

pub(crate) fn execution_tiny_recent_orders(
    store: &SqliteDiscoveryStore,
    since: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ExecutionTinyProofOrder>> {
    let sql = if metadata_query_is_available(store)? {
        RECENT_ORDERS_SQL
    } else {
        RECENT_ORDERS_WITHOUT_METADATA_SQL
    };
    let mut stmt = store
        .conn
        .prepare(sql)
        .context("failed to prepare execution tiny recent orders query")?;
    let rows = stmt
        .query_map(
            params![since.to_rfc3339(), i64::from(limit.max(1))],
            read_recent_order,
        )
        .context("failed querying execution tiny recent orders")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading execution tiny recent orders")
}

pub(crate) fn execution_tiny_open_positions(
    store: &SqliteDiscoveryStore,
    limit: u32,
) -> Result<Vec<ExecutionTinyProofOpenPosition>> {
    let mut stmt = store
        .conn
        .prepare(
            "SELECT position_id, token, qty, cost_sol, opened_ts
             FROM positions
             WHERE accounting_bucket = ?1
               AND state = ?2
             ORDER BY opened_ts DESC, position_id DESC
             LIMIT ?3",
        )
        .context("failed to prepare execution tiny open positions query")?;
    let rows = stmt
        .query_map(
            params![
                EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                EXECUTION_CANARY_POSITION_STATE_OPEN,
                i64::from(limit.max(1)),
            ],
            |row| {
                let opened_raw: String = row.get(4)?;
                Ok(ExecutionTinyProofOpenPosition {
                    position_id: row.get(0)?,
                    token: row.get(1)?,
                    qty: row.get(2)?,
                    cost_sol: row.get(3)?,
                    opened_ts: parse_ts_sql(4, &opened_raw)?,
                })
            },
        )
        .context("failed querying execution tiny open positions")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("failed reading execution tiny open positions")
}

fn metadata_query_is_available(store: &SqliteDiscoveryStore) -> Result<bool> {
    if !store.sqlite_table_exists("execution_canary_build_plan_metadata")? {
        return Ok(false);
    }
    for column in [
        "quote_event_id",
        "quote_source",
        "priority_fee_lamports",
        "decision_status",
        "decision_reason",
        "recorded_ts",
    ] {
        if !column_exists(store, "execution_canary_build_plan_metadata", column)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn read_proof_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProofRow> {
    let opened_raw: String = row.get(5)?;
    let closed_raw: String = row.get(6)?;
    let buy_quote = read_quote_side(row, 7)?;
    let sell_quote = read_quote_side(row, 17)?;
    Ok(ProofRow {
        shadow_closed_trade_id: row.get(0)?,
        signal_id: row.get(1)?,
        wallet_id: row.get(2)?,
        token: row.get(3)?,
        shadow_pnl_sol: row.get(4)?,
        opened_ts: parse_ts_sql(5, &opened_raw)?,
        closed_ts: parse_ts_sql(6, &closed_raw)?,
        buy_order: read_order_side(row, 27, "buy", buy_quote.signal_ts, buy_quote.request_ts)?,
        sell_order: read_order_side(row, 44, "sell", sell_quote.signal_ts, sell_quote.request_ts)?,
        buy_quote,
        sell_quote,
        position: read_position_side(row, 61)?,
    })
}

fn read_quote_side(row: &rusqlite::Row<'_>, base: usize) -> rusqlite::Result<QuoteSide> {
    Ok(QuoteSide {
        event_id: row.get(base)?,
        quote_status: row.get(base + 2)?,
        decision_status: row.get(base + 3)?,
        decision_reason: row.get(base + 4)?,
        request_ts: optional_ts(row, base + 5)?,
        signal_ts: optional_ts(row, base + 6)?,
        quote_latency_ms: optional_i64_to_u64_sql(row.get(base + 7)?, base + 7)?,
        decision_delay_ms: optional_i64_to_u64_sql(row.get(base + 8)?, base + 8)?,
        priority_fee_lamports: optional_i64_to_u64_sql(row.get(base + 9)?, base + 9)?,
    })
}

fn read_order_side(
    row: &rusqlite::Row<'_>,
    base: usize,
    side: &str,
    signal_ts: Option<DateTime<Utc>>,
    quote_ts: Option<DateTime<Utc>>,
) -> rusqlite::Result<Option<ExecutionTinyProofOrder>> {
    let order_id: Option<String> = row.get(base)?;
    let Some(order_id) = order_id else {
        return Ok(None);
    };
    let submit_ts = required_ts(row, base + 3)?;
    let confirm_ts = optional_ts(row, base + 4)?;
    let tx_signature: Option<String> = row.get(base + 5)?;
    let attempt_raw: i64 = row.get(base + 8)?;
    let attempt = u32::try_from(attempt_raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            base + 8,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(Some(ExecutionTinyProofOrder {
        order_id,
        signal_id: row.get(base + 1)?,
        side: Some(side.to_string()),
        token: row.get(base + 9)?,
        route: row.get(base + 10)?,
        status: row.get(base + 2)?,
        err_code: row.get(base + 11)?,
        attempt,
        submit_ts,
        confirm_ts,
        tx_signature_present: tx_signature
            .as_deref()
            .is_some_and(|sig| !sig.trim().is_empty()),
        simulation_status: row.get(base + 6)?,
        simulation_error: row.get(base + 7)?,
        submit_to_confirm_ms: confirm_ts.map(|confirm_ts| delta_ms(submit_ts, confirm_ts)),
        signal_to_submit_ms: signal_ts.map(|signal_ts| delta_ms(signal_ts, submit_ts)),
        quote_to_submit_ms: quote_ts.map(|quote_ts| delta_ms(quote_ts, submit_ts)),
        quote_source: row.get(base + 12)?,
        quote_event_id: row.get(base + 13)?,
        priority_fee_lamports: optional_i64_to_u64_sql(row.get(base + 14)?, base + 14)?,
        decision_status: row.get(base + 15)?,
        decision_reason: row.get(base + 16)?,
    }))
}

fn read_position_side(row: &rusqlite::Row<'_>, base: usize) -> rusqlite::Result<PositionSide> {
    Ok(PositionSide {
        state: row.get(base)?,
        opened_ts: optional_ts(row, base + 1)?,
        closed_ts: optional_ts(row, base + 2)?,
        cost_sol: row.get(base + 3)?,
        pnl_sol: row.get(base + 4)?,
    })
}

fn read_recent_order(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionTinyProofOrder> {
    let submit_ts = required_ts(row, 7)?;
    let confirm_ts = optional_ts(row, 8)?;
    let tx_signature: Option<String> = row.get(9)?;
    let attempt_raw: i64 = row.get(12)?;
    let attempt = u32::try_from(attempt_raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    let signal_ts = optional_ts(row, 18)?;
    let metadata_ts = optional_ts(row, 19)?;
    Ok(ExecutionTinyProofOrder {
        order_id: row.get(0)?,
        signal_id: row.get(1)?,
        side: row.get(2)?,
        token: row.get(3)?,
        status: row.get(4)?,
        err_code: row.get(5)?,
        route: row.get(6)?,
        submit_ts,
        confirm_ts,
        tx_signature_present: tx_signature
            .as_deref()
            .is_some_and(|sig| !sig.trim().is_empty()),
        simulation_status: row.get(10)?,
        simulation_error: row.get(11)?,
        attempt,
        submit_to_confirm_ms: confirm_ts.map(|confirm_ts| delta_ms(submit_ts, confirm_ts)),
        signal_to_submit_ms: signal_ts.map(|signal_ts| delta_ms(signal_ts, submit_ts)),
        quote_to_submit_ms: metadata_ts.map(|metadata_ts| delta_ms(metadata_ts, submit_ts)),
        quote_source: row.get(13)?,
        quote_event_id: row.get(14)?,
        priority_fee_lamports: optional_i64_to_u64_sql(row.get(15)?, 15)?,
        decision_status: row.get(16)?,
        decision_reason: row.get(17)?,
    })
}

fn required_ts(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<DateTime<Utc>> {
    let raw: String = row.get(index)?;
    parse_ts_sql(index, &raw)
}

fn optional_ts(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    let raw: Option<String> = row.get(index)?;
    raw.as_deref()
        .map(|raw| parse_ts_sql(index, raw))
        .transpose()
}

fn parse_ts_sql(index: usize, raw: &str) -> rusqlite::Result<DateTime<Utc>> {
    parse_rfc3339_utc(raw, "execution tiny proof timestamp").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        )
    })
}

fn optional_i64_to_u64_sql(value: Option<i64>, index: usize) -> rusqlite::Result<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })
        })
        .transpose()
}

fn delta_ms(start: DateTime<Utc>, end: DateTime<Utc>) -> i64 {
    end.signed_duration_since(start).num_milliseconds()
}

const PROOF_ROWS_SQL: &str = "
SELECT
    closed.id,
    closed.signal_id,
    closed.wallet_id,
    closed.token,
    closed.pnl_sol,
    closed.opened_ts,
    closed.closed_ts,
    buy.event_id,
    buy.signal_id,
    buy.quote_status,
    buy.decision_status,
    buy.decision_reason,
    buy.request_ts,
    buy.signal_ts,
    buy.quote_latency_ms,
    buy.decision_delay_ms,
    buy.priority_fee_lamports,
    sell.event_id,
    sell.signal_id,
    sell.quote_status,
    sell.decision_status,
    sell.decision_reason,
    sell.request_ts,
    sell.signal_ts,
    sell.quote_latency_ms,
    sell.decision_delay_ms,
    sell.priority_fee_lamports,
    buy_order.order_id,
    buy_order.signal_id,
    buy_order.status,
    buy_order.submit_ts,
    buy_order.confirm_ts,
    buy_order.tx_signature,
    buy_order.simulation_status,
    buy_order.simulation_error,
    buy_order.attempt,
    copy_buy.token,
    buy_order.route,
    buy_order.err_code,
    buy_metadata.quote_source,
    buy_metadata.quote_event_id,
    buy_metadata.priority_fee_lamports,
    buy_metadata.decision_status,
    buy_metadata.decision_reason,
    sell_order.order_id,
    sell_order.signal_id,
    sell_order.status,
    sell_order.submit_ts,
    sell_order.confirm_ts,
    sell_order.tx_signature,
    sell_order.simulation_status,
    sell_order.simulation_error,
    sell_order.attempt,
    copy_sell.token,
    sell_order.route,
    sell_order.err_code,
    sell_metadata.quote_source,
    sell_metadata.quote_event_id,
    sell_metadata.priority_fee_lamports,
    sell_metadata.decision_status,
    sell_metadata.decision_reason,
    pos.state,
    pos.opened_ts,
    pos.closed_ts,
    pos.cost_sol,
    pos.pnl_sol
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
LEFT JOIN orders AS buy_order
    ON buy_order.signal_id = buy.signal_id
   AND buy_order.order_id LIKE 'exec-canary:%'
LEFT JOIN copy_signals AS copy_buy
    ON copy_buy.signal_id = buy_order.signal_id
LEFT JOIN execution_canary_build_plan_metadata AS buy_metadata
    ON buy_metadata.order_id = buy_order.order_id
LEFT JOIN orders AS sell_order
    ON sell_order.signal_id = sell.signal_id
   AND sell_order.order_id LIKE 'exec-canary:%'
LEFT JOIN copy_signals AS copy_sell
    ON copy_sell.signal_id = sell_order.signal_id
LEFT JOIN execution_canary_build_plan_metadata AS sell_metadata
    ON sell_metadata.order_id = sell_order.order_id
LEFT JOIN positions AS pos
    ON pos.position_id = 'exec-canary-pos:' || buy_order.order_id
   AND pos.accounting_bucket = 'execution_canary'
WHERE closed.closed_ts >= ?1
  AND COALESCE(closed.close_context, 'market') = 'market'
  AND closed.signal_id NOT LIKE 'stale-close-%'
ORDER BY closed.closed_ts DESC, closed.id DESC
LIMIT ?2";

const RECENT_ORDERS_SQL: &str = "
SELECT
    orders.order_id,
    orders.signal_id,
    copy_signals.side,
    copy_signals.token,
    orders.status,
    orders.err_code,
    orders.route,
    orders.submit_ts,
    orders.confirm_ts,
    orders.tx_signature,
    orders.simulation_status,
    orders.simulation_error,
    orders.attempt,
    metadata.quote_source,
    metadata.quote_event_id,
    metadata.priority_fee_lamports,
    metadata.decision_status,
    metadata.decision_reason,
    copy_signals.ts,
    metadata.recorded_ts
FROM orders
LEFT JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
LEFT JOIN execution_canary_build_plan_metadata AS metadata
    ON metadata.order_id = orders.order_id
WHERE orders.order_id LIKE 'exec-canary:%'
  AND orders.submit_ts >= ?1
ORDER BY orders.submit_ts DESC, orders.order_id DESC
LIMIT ?2";

const RECENT_ORDERS_WITHOUT_METADATA_SQL: &str = "
SELECT
    orders.order_id,
    orders.signal_id,
    copy_signals.side,
    copy_signals.token,
    orders.status,
    orders.err_code,
    orders.route,
    orders.submit_ts,
    orders.confirm_ts,
    orders.tx_signature,
    orders.simulation_status,
    orders.simulation_error,
    orders.attempt,
    NULL AS quote_source,
    NULL AS quote_event_id,
    NULL AS priority_fee_lamports,
    NULL AS decision_status,
    NULL AS decision_reason,
    copy_signals.ts,
    NULL AS recorded_ts
FROM orders
LEFT JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
WHERE orders.order_id LIKE 'exec-canary:%'
  AND orders.submit_ts >= ?1
ORDER BY orders.submit_ts DESC, orders.order_id DESC
LIMIT ?2";
