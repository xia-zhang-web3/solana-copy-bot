use crate::{
    execution_canary_rows::execution_canary_order_from_row, ExecutionCanaryOrder,
    ExecutionRetryCursor, ExecutionRetryOrder, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{Context, Result};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_reconcilable_execution_canary_orders_for_route(
        &self,
        route: &str,
        retry_reason: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        self.list_reconcilable_execution_canary_orders_with_buy_retry_guard(
            route,
            retry_reason,
            limit,
            None,
        )
    }

    /// Keep recovery bounded when BUY entry is blocked. Only unsigned SIMULATED
    /// BUY retries within budget are deferred; receipts, SELL and expiry remain eligible.
    pub fn list_reconcilable_execution_canary_orders_with_buy_retry_guard(
        &self,
        route: &str,
        retry_reason: &str,
        limit: u32,
        blocked_buy_retry_max_attempt: Option<u32>,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        self.list_reconcilable_execution_canary_orders_for_retry_reasons(
            route,
            retry_reason,
            None,
            limit,
            blocked_buy_retry_max_attempt,
        )
    }

    /// Combine supported retry groups with receipt recovery under one age order and limit.
    /// A missing additional reason preserves the single-reason query contract.
    pub fn list_reconcilable_execution_canary_orders_for_retry_reasons(
        &self,
        route: &str,
        retry_reason: &str,
        additional_retry_reason: Option<&str>,
        limit: u32,
        blocked_buy_retry_max_attempt: Option<u32>,
    ) -> Result<Vec<ExecutionCanaryOrder>> {
        Ok(self
            .list_reconcilable_execution_canary_orders_page(
                route,
                retry_reason,
                additional_retry_reason,
                limit,
                blocked_buy_retry_max_attempt,
                None,
            )?
            .into_iter()
            .map(|r| r.order)
            .collect())
    }

    /// Same eligibility and priority; continuation excludes only an already visited prefix.
    pub fn list_reconcilable_execution_canary_orders_page(
        &self,
        route: &str,
        retry_reason: &str,
        additional_retry_reason: Option<&str>,
        limit: u32,
        blocked_buy_retry_max_attempt: Option<u32>,
        after: Option<&ExecutionRetryCursor>,
    ) -> Result<Vec<ExecutionRetryOrder>> {
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
                    attempt,
                    COALESCE((SELECT last_attempt_at FROM execution_canary_reconcile_attempts a WHERE a.order_id=orders.order_id), (SELECT last_attempt_at FROM execution_canary_receipt_proofs p
                        WHERE p.order_id=orders.order_id), submit_ts)
                 FROM orders
                 WHERE order_id LIKE 'exec-canary:%'
                   AND route = ?1
                   AND (
                       EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch d WHERE d.order_id=orders.order_id)
                       OR status = ?2
                       OR status = ?6
                       OR (status = ?7 AND NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id = orders.order_id))
                       OR (
                           status = ?3
                           AND (tx_signature IS NULL OR TRIM(tx_signature) = '')
                           AND (
                               simulation_error = ?4
                               OR simulation_error LIKE ?4 || ':%'
                               OR (?9 IS NOT NULL AND (
                                   simulation_error = ?9
                                   OR simulation_error LIKE ?9 || ':%'
                               ))
                           )
                       )
                   )
                   AND (?8 IS NULL OR NOT (
                       status = ?3
                       AND (tx_signature IS NULL OR TRIM(tx_signature) = '')
                       AND attempt <= ?8
                       AND EXISTS (SELECT 1 FROM copy_signals s
                                   WHERE s.signal_id = orders.signal_id AND lower(s.side) = 'buy')
                   ))
                   AND (?10 IS NULL OR (COALESCE((SELECT last_attempt_at FROM execution_canary_reconcile_attempts a WHERE a.order_id=orders.order_id), (SELECT last_attempt_at FROM execution_canary_receipt_proofs p
                       WHERE p.order_id=orders.order_id),submit_ts),submit_ts,order_id) > (?10,?11,?12))
                 ORDER BY COALESCE((SELECT last_attempt_at FROM execution_canary_reconcile_attempts a WHERE a.order_id=orders.order_id), (SELECT last_attempt_at FROM execution_canary_receipt_proofs p
                                    WHERE p.order_id = orders.order_id), submit_ts) ASC,
                          submit_ts ASC, order_id ASC
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
                    crate::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
                    crate::EXECUTION_STATUS_CANARY_CONFIRMED,
                    blocked_buy_retry_max_attempt.map(i64::from),
                    additional_retry_reason,
                    after.map(|c| c.first.as_str()),
                    after.map(|c| c.second.as_str()),
                    after.map(|c| c.third.as_str()),
                ],
                |row| {
                    Ok(ExecutionRetryOrder {
                        order: execution_canary_order_from_row(row)?,
                        cursor: ExecutionRetryCursor {
                            first: row.get(12)?,
                            second: row.get(3)?,
                            third: row.get(0)?,
                        },
                    })
                },
            )
            .context("failed querying submitted execution canary orders")?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("failed reading submitted execution canary orders")
    }
}
