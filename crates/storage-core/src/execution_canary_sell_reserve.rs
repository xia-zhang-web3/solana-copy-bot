use crate::{
    execution_canary_state::{execution_canary_client_order_id, execution_canary_order_id},
    ExecutionCanaryRecordOutcome, ExecutionCanarySellReserveResult, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_SIMULATED, EXECUTION_STATUS_CANARY_SUBMITTED,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension};

const SELL_IN_FLIGHT_GUARD_SECONDS: i64 = 300;

enum GuardedSellReserveTx {
    Inserted(String),
    Existing(String),
    BlockedByInFlight(String),
}

impl SqliteDiscoveryStore {
    pub fn reserve_execution_canary_sell_order_unless_token_in_flight(
        &self,
        signal_id: &str,
        route: &str,
        now: DateTime<Utc>,
    ) -> Result<ExecutionCanarySellReserveResult> {
        let order_id = execution_canary_order_id(signal_id);
        let client_order_id = execution_canary_client_order_id(signal_id);
        let now_rfc3339 = now.to_rfc3339();
        let cutoff = (now - Duration::seconds(SELL_IN_FLIGHT_GUARD_SECONDS)).to_rfc3339();
        let tx_result = self.with_immediate_transaction_retry(
            "execution canary guarded sell order reserve",
            |conn| {
                let existing: Option<String> = conn
                    .query_row(
                        "SELECT order_id FROM orders WHERE order_id = ?1 LIMIT 1",
                        params![order_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed checking existing guarded sell order")?;
                if let Some(existing) = existing {
                    return Ok(GuardedSellReserveTx::Existing(existing));
                }

                let (token, side): (String, String) = conn
                    .query_row(
                        "SELECT token, side
                         FROM copy_signals
                         WHERE signal_id = ?1
                         LIMIT 1",
                        params![signal_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .context("failed loading guarded sell signal")?
                    .ok_or_else(|| anyhow!("missing copy signal for guarded sell {signal_id}"))?;
                if !side.eq_ignore_ascii_case("sell") {
                    return Err(anyhow!(
                        "guarded sell reserve requires sell signal {signal_id}, got {side}"
                    ));
                }

                let blocker: Option<String> = conn
                    .query_row(
                        "SELECT orders.order_id
                         FROM orders
                         JOIN copy_signals AS signal
                           ON signal.signal_id = orders.signal_id
                         WHERE orders.order_id LIKE 'exec-canary:%'
                           AND orders.route = ?1
                           AND orders.order_id != ?2
                           AND signal.token = ?3
                           AND lower(signal.side) = 'sell'
                           AND orders.status IN (?4, ?5, ?6, ?7)
                           AND orders.submit_ts >= ?8
                         ORDER BY orders.submit_ts ASC, orders.order_id ASC
                         LIMIT 1",
                        params![
                            route,
                            order_id,
                            token,
                            EXECUTION_STATUS_CANARY_CANDIDATE,
                            EXECUTION_STATUS_CANARY_BUILT,
                            EXECUTION_STATUS_CANARY_SIMULATED,
                            EXECUTION_STATUS_CANARY_SUBMITTED,
                            cutoff,
                        ],
                        |row| row.get(0),
                    )
                    .optional()
                    .context("failed checking in-flight guarded sell order")?;
                if let Some(blocker) = blocker {
                    return Ok(GuardedSellReserveTx::BlockedByInFlight(blocker));
                }

                conn.execute(
                    "INSERT INTO orders(
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
                    ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL, ?6, NULL, ?7, NULL, 1)",
                    params![
                        order_id,
                        signal_id,
                        route,
                        now_rfc3339,
                        EXECUTION_STATUS_CANARY_CANDIDATE,
                        client_order_id,
                        crate::EXECUTION_SIMULATION_STATUS_NOT_RUN,
                    ],
                )
                .context("failed inserting guarded sell order")?;
                Ok(GuardedSellReserveTx::Inserted(order_id.clone()))
            },
        )?;

        let (order_id, outcome, blocked_by_in_flight_sell) = match tx_result {
            GuardedSellReserveTx::Inserted(order_id) => {
                (order_id, ExecutionCanaryRecordOutcome::Inserted, false)
            }
            GuardedSellReserveTx::Existing(order_id) => {
                (order_id, ExecutionCanaryRecordOutcome::Existing, false)
            }
            GuardedSellReserveTx::BlockedByInFlight(order_id) => {
                (order_id, ExecutionCanaryRecordOutcome::Existing, true)
            }
        };
        let order = self
            .load_execution_canary_order(&order_id)?
            .ok_or_else(|| anyhow!("missing guarded sell order after reserve {order_id}"))?;
        Ok(ExecutionCanarySellReserveResult {
            outcome,
            order,
            blocked_by_in_flight_sell,
        })
    }
}
