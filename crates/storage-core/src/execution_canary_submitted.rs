use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
    EXECUTION_SIMULATION_STATUS_NOT_RUN, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{Context, Result};
use rusqlite::params;

const QUOTE_STATUS_OK: &str = "ok";
const DECISION_WOULD_EXECUTE: &str = "would_execute";
const DECISION_WOULD_FORCE_EXIT: &str = "would_force_exit";

impl SqliteDiscoveryStore {
    pub fn list_reconcilable_execution_canary_orders_for_route(
        &self,
        route: &str,
        retry_reason: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    order_id,
                    signal_id,
                    route,
                    submit_ts,
                    confirm_ts,
                    status,
                    err_code,
                    client_order_id,
                    tx_signature,
                    simulation_status,
                    simulation_error,
                    attempt
                 FROM orders
                 WHERE order_id LIKE 'exec-canary:%'
                   AND route = ?1
                   AND (
                       status = ?2
                       OR (
                           status = ?3
                           AND (tx_signature IS NULL OR TRIM(tx_signature) = '')
                           AND (
                               simulation_error = ?4
                               OR simulation_error LIKE ?4 || ':%'
                           )
                       )
                   )
                 ORDER BY submit_ts ASC, order_id ASC
                 LIMIT ?5",
            )
            .context("failed to prepare submitted execution canary order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_SUBMITTED,
                    EXECUTION_STATUS_CANARY_SIMULATED,
                    retry_reason,
                    i64::from(limit),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying submitted execution canary orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading submitted execution canary orders")
    }

    pub fn list_failed_simulation_sell_execution_canary_orders_for_route(
        &self,
        route: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    orders.order_id,
                    orders.signal_id,
                    orders.route,
                    orders.submit_ts,
                    orders.confirm_ts,
                    orders.status,
                    orders.err_code,
                    orders.client_order_id,
                    orders.tx_signature,
                    orders.simulation_status,
                    orders.simulation_error,
                    orders.attempt
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.err_code = ?3
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'sell'
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        WHERE pos.token = copy_signals.token
                          AND pos.accounting_bucket = ?4
                          AND pos.state = ?5
                   )
                 ORDER BY orders.submit_ts ASC, orders.order_id ASC
                 LIMIT ?6",
            )
            .context("failed to prepare failed simulation sell order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_FAILED,
                    EXECUTION_ERROR_SIMULATION_FAILED,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying failed simulation sell orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading failed simulation sell orders")
    }

    pub fn list_failed_simulation_buy_execution_canary_orders_for_route(
        &self,
        route: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    orders.order_id,
                    orders.signal_id,
                    orders.route,
                    orders.submit_ts,
                    orders.confirm_ts,
                    orders.status,
                    orders.err_code,
                    orders.client_order_id,
                    orders.tx_signature,
                    orders.simulation_status,
                    orders.simulation_error,
                    orders.attempt
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.err_code = ?3
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'buy'
                 ORDER BY orders.submit_ts ASC, orders.order_id ASC
                 LIMIT ?4",
            )
            .context("failed to prepare failed simulation buy order query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_FAILED,
                    EXECUTION_ERROR_SIMULATION_FAILED,
                    i64::from(limit.max(1)),
                ],
                execution_canary_order_from_row,
            )
            .context("failed querying failed simulation buy orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading failed simulation buy orders")
    }

    pub fn list_retry_candidate_sell_execution_quote_event_ids_for_route(
        &self,
        route: &str,
        retry_reason: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event.event_id
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 JOIN execution_quote_canary_events AS event
                   ON event.signal_id = orders.signal_id
                  AND lower(event.side) = 'sell'
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.simulation_status = ?3
                   AND orders.simulation_error = ?4
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'sell'
                   AND event.quote_status = ?5
                   AND event.decision_status IN (?6, ?7)
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        WHERE pos.token = copy_signals.token
                          AND pos.accounting_bucket = ?8
                          AND pos.state = ?9
                          AND (
                              (
                                  pos.position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                                  AND event.request_ts >= pos.opened_ts
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM orders AS buy_order
                                  WHERE pos.position_id = 'exec-canary-pos:' || buy_order.order_id
                                    AND event.request_ts >= pos.opened_ts
                              )
                          )
                   )
                 ORDER BY orders.submit_ts ASC, event.request_ts DESC, event.event_id DESC
                 LIMIT ?10",
            )
            .context("failed to prepare retry candidate sell quote event query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    retry_reason,
                    QUOTE_STATUS_OK,
                    DECISION_WOULD_EXECUTE,
                    DECISION_WOULD_FORCE_EXIT,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                |row| row.get(0),
            )
            .context("failed querying retry candidate sell quote events")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading retry candidate sell quote events")
    }

    pub fn list_failed_build_sell_execution_quote_event_ids_for_route(
        &self,
        route: &str,
        limit: u32,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event.event_id
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 JOIN execution_quote_canary_events AS event
                   ON event.signal_id = orders.signal_id
                  AND lower(event.side) = 'sell'
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.err_code = ?3
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND lower(copy_signals.side) = 'sell'
                   AND event.quote_status = ?4
                   AND event.decision_status IN (?5, ?6)
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        WHERE pos.token = copy_signals.token
                          AND pos.accounting_bucket = ?7
                          AND pos.state = ?8
                          AND (
                              (
                                  pos.position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                                  AND event.request_ts >= pos.opened_ts
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM orders AS buy_order
                                  WHERE pos.position_id = 'exec-canary-pos:' || buy_order.order_id
                                    AND event.request_ts >= pos.opened_ts
                              )
                          )
                   )
                 ORDER BY orders.submit_ts ASC, event.request_ts DESC, event.event_id DESC
                 LIMIT ?9",
            )
            .context("failed to prepare failed build sell quote event query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_FAILED,
                    EXECUTION_ERROR_BUILD_FAILED,
                    QUOTE_STATUS_OK,
                    DECISION_WOULD_EXECUTE,
                    DECISION_WOULD_FORCE_EXIT,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                |row| row.get(0),
            )
            .context("failed querying failed build sell quote events")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading failed build sell quote events")
    }

    pub fn list_terminal_no_route_sell_execution_quote_event_ids_for_route(
        &self,
        route: &str,
        retry_after: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event.event_id
                 FROM orders
                 JOIN copy_signals ON copy_signals.signal_id = orders.signal_id
                 JOIN execution_quote_canary_events AS event
                   ON event.signal_id = orders.signal_id
                  AND lower(event.side) = 'sell'
                 WHERE orders.order_id LIKE 'exec-canary:%'
                   AND orders.route = ?1
                   AND orders.status = ?2
                   AND orders.err_code = ?3
                   AND (orders.tx_signature IS NULL OR TRIM(orders.tx_signature) = '')
                   AND COALESCE(orders.confirm_ts, orders.submit_ts) <= ?4
                   AND lower(copy_signals.side) = 'sell'
                   AND event.quote_status = ?5
                   AND event.decision_status IN (?6, ?7)
                   AND EXISTS (
                        SELECT 1
                        FROM positions AS pos
                        WHERE pos.token = copy_signals.token
                          AND pos.accounting_bucket = ?8
                          AND pos.state = ?9
                          AND (
                              (
                                  pos.position_id LIKE 'exec-canary-pos:recovery-orphan:%'
                                  AND event.request_ts >= pos.opened_ts
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM orders AS buy_order
                                  WHERE pos.position_id = 'exec-canary-pos:' || buy_order.order_id
                                    AND event.request_ts >= pos.opened_ts
                              )
                          )
                   )
                 ORDER BY COALESCE(orders.confirm_ts, orders.submit_ts) ASC,
                          event.request_ts DESC,
                          event.event_id DESC
                 LIMIT ?10",
            )
            .context("failed to prepare terminal no-route sell quote event query")?;
        let rows = stmt
            .query_map(
                params![
                    route,
                    EXECUTION_STATUS_CANARY_FAILED,
                    EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE,
                    retry_after.to_rfc3339(),
                    QUOTE_STATUS_OK,
                    DECISION_WOULD_EXECUTE,
                    DECISION_WOULD_FORCE_EXIT,
                    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    EXECUTION_CANARY_POSITION_STATE_OPEN,
                    i64::from(limit.max(1)),
                ],
                |row| row.get(0),
            )
            .context("failed querying terminal no-route sell quote events")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading terminal no-route sell quote events")
    }
}
